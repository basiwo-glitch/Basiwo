'use strict';
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json({ limit: '32kb' }));
app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

const PORT = Number(process.env.PORT || 3000);
const API_KEY = (process.env.PUMPPORTAL_API_KEY || '').trim();
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data', 'state.json');
const START_CASH = 1000;
const MAX_POSITIONS = 4;
const TRADE_SIZE = 50;
const STOP_LOSS_PCT = 12;
const TAKE_PROFIT_PCT = 25;
const MAX_TOKEN_AGE_MS = 15 * 60 * 1000;
const MIN_LIQUIDITY_USD = 15000;
const MIN_VOLUME_USD = 20000;
const MAX_ENTRY_CHANGE_PCT = 35;
const DEX = 'https://api.dexscreener.com';

let state = loadState();
let pumpWs = null, reconnectTimer = null, marketTimer = null, feedStatus = 'starting';
let latestTokens = new Map();
let lastFeedMessageAt = null;
let lastMarketPollAt = null;
let scanCount = 0;
let autoEnabled = true;
let seenMints = new Set(state.seenMints || []);
const clients = new Set();

function loadState() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const x = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      if (x && typeof x.cash === 'number' && x.positions && Array.isArray(x.history)) {
        return { cash: x.cash, initialCash: x.initialCash || START_CASH, positions: x.positions, history: x.history, seenMints: x.seenMints || [], autoEnabled: x.autoEnabled !== false };
      }
    }
  } catch (e) { console.error('Could not load persisted state:', e.message); }
  return { cash: START_CASH, initialCash: START_CASH, positions: {}, history: [], seenMints: [], autoEnabled: true };
}
function persist() {
  try {
    fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
    state.seenMints = [...seenMints].slice(-3000);
    fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2));
  } catch (e) { console.error('Persist failed:', e.message); }
}
function publicState() {
  const positions = Object.values(state.positions).map(p => ({...p, marketValue: (p.currentPrice || p.entryPrice) * p.qty, pnl: ((p.currentPrice || p.entryPrice) - p.entryPrice) * p.qty }));
  const unrealized = positions.reduce((s,p)=>s+p.pnl,0);
  const realized = state.history.filter(t=>t.type==='SELL').reduce((s,t)=>s+(Number(t.pnl)||0),0);
  const value = state.cash + positions.reduce((s,p)=>s+p.marketValue,0);
  return { cash:state.cash, initialCash:state.initialCash, portfolioValue:value, pnl:unrealized+realized, positions, history:state.history.slice(0,60), tokens:[...latestTokens.values()].slice(0,60), feedStatus, lastFeedMessageAt, lastMarketPollAt, scanCount, autoEnabled, risk:{maxPositions:MAX_POSITIONS,tradeSize:TRADE_SIZE,stopLossPct:STOP_LOSS_PCT,takeProfitPct:TAKE_PROFIT_PCT,minLiquidityUsd:MIN_LIQUIDITY_USD,minVolumeUsd:MIN_VOLUME_USD} };
}
function broadcast(type='state', extra={}) {
  const message = JSON.stringify({type, data:publicState(), ...extra});
  for (const ws of clients) if (ws.readyState===WebSocket.OPEN) ws.send(message);
}
function logTrade(trade) { state.history.unshift({...trade, time:new Date().toISOString()}); state.history=state.history.slice(0,500); persist(); }
function safeNum(v) { const n=Number(v); return Number.isFinite(n)?n:0; }
function scoreToken(t) {
  let score=0;
  const liq=safeNum(t.liquidityUsd), vol=safeNum(t.volume24h), buys=safeNum(t.buys24h), sells=safeNum(t.sells24h), change=safeNum(t.change24h);
  score += Math.max(0, Math.min(25, (Math.log10(Math.max(1,liq))-3)*10));
  score += Math.max(0, Math.min(25, (Math.log10(Math.max(1,vol))-3)*8));
  score += change>0 ? Math.min(25,change*0.8) : Math.max(0, 5+change*0.2);
  const tx=buys+sells;
  score += tx ? Math.max(0,Math.min(25,((buys/tx)-0.35)*80)) : 0;
  if (change>MAX_ENTRY_CHANGE_PCT) score-=25;
  if (change < -15) score-=15;
  if (tx<40) score-=10;
  if (liq<MIN_LIQUIDITY_USD) score-=30;
  if (vol<MIN_VOLUME_USD) score-=15;
  return Math.max(0,Math.min(99,Math.round(score)));
}
function eligible(t) {
  const age=Date.now()-t.createdAt;
  return autoEnabled && !state.positions[t.address] && !seenMints.has(t.address) &&
    age>=0 && age<=MAX_TOKEN_AGE_MS &&
    t.liquidityUsd>=MIN_LIQUIDITY_USD && t.volume24h>=MIN_VOLUME_USD &&
    t.change24h<=MAX_ENTRY_CHANGE_PCT && t.change24h>=-15 &&
    t.buys24h+t.sells24h>=40 && t.score>=65 && t.priceUsd>0;
}
function buyPaper(t) {
  if (!eligible(t)) return false;
  const positions=Object.keys(state.positions).length;
  const amount=Math.min(TRADE_SIZE,state.cash);
  if (positions>=MAX_POSITIONS || amount<10) return false;
  // Conservative paper fill: 1% execution penalty to approximate fees/slippage.
  const entryPrice=t.priceUsd*1.01;
  state.cash-=amount;
  state.positions[t.address]={address:t.address,name:t.name,symbol:t.symbol,entryPrice,currentPrice:t.priceUsd,qty:amount/entryPrice,cost:amount,createdAt:t.createdAt,openedAt:new Date().toISOString(),url:t.url||`https://dexscreener.com/solana/${t.address}`,score:t.score};
  seenMints.add(t.address);
  logTrade({type:'BUY',symbol:t.symbol,address:t.address,amount,price:entryPrice,reason:'Auto Paper Entry · filters passed'});
  broadcast('trade',{trade:state.history[0]});
  return true;
}
function sellPaper(address, reason) {
  const p=state.positions[address]; if(!p) return;
  const exitPrice=(p.currentPrice||p.entryPrice)*0.99;
  const proceeds=p.qty*exitPrice, pnl=proceeds-p.cost;
  state.cash+=proceeds; delete state.positions[address];
  logTrade({type:'SELL',symbol:p.symbol,address,amount:proceeds,price:exitPrice,pnl,reason});
  broadcast('trade',{trade:state.history[0]});
}
function onTokenCreated(msg) {
  const address=msg.mint || msg.tokenAddress || msg.address;
  if(!address || seenMints.has(address)) return;
  const token={address,name:String(msg.name||'New token').slice(0,80),symbol:String(msg.symbol||'???').slice(0,16),uri:msg.uri||'',createdAt:Date.now(),source:'PumpPortal WebSocket',priceUsd:0,marketCapSol:safeNum(msg.marketCapSol),score:0,liquidityUsd:0,volume24h:0,buys24h:0,sells24h:0,change24h:0,url:`https://dexscreener.com/solana/${address}`,status:'new'};
  latestTokens.set(address,token);
  while(latestTokens.size>1500) latestTokens.delete(latestTokens.keys().next().value);
  lastFeedMessageAt=new Date().toISOString();
  scanCount++;
  // New-token event alone does not prove liquidity, price quality, or sellability.
  // Token is eligible only after DEX Screener supplies independent market data.
  broadcast('new-token',{token});
}
async function connectPumpPortal() {
  if (pumpWs && (pumpWs.readyState===WebSocket.OPEN || pumpWs.readyState===WebSocket.CONNECTING)) return;
  clearTimeout(reconnectTimer);
  feedStatus='connecting'; broadcast('status');
  pumpWs = new WebSocket('wss://pumpportal.fun/api/data' + (API_KEY ? '?api-key=' + encodeURIComponent(API_KEY) : ''));
  pumpWs.on('open',()=> {
    feedStatus='connected'; broadcast('status');
    pumpWs.send(JSON.stringify({method:'subscribeNewToken'}));
    console.log('PumpPortal WebSocket connected; subscribed to new tokens.');
  });
  pumpWs.on('message',buf=>{
    let msg; try{msg=JSON.parse(buf.toString())}catch{return}
    if (msg && (msg.mint || msg.tokenAddress || msg.address)) onTokenCreated(msg);
  });
  pumpWs.on('close',()=>{feedStatus='disconnected';broadcast('status');reconnectTimer=setTimeout(connectPumpPortal,5000)});
  pumpWs.on('error',err=>{console.error('PumpPortal WebSocket:',err.message);feedStatus='error';broadcast('status');try{pumpWs.close()}catch{}});
}
async function fetchJson(url) {
  const r=await fetch(url,{headers:{'accept':'application/json'},signal:AbortSignal.timeout(9000)});
  if(!r.ok) throw new Error(`HTTP ${r.status} from market API`);
  return r.json();
}
async function pollMarket() {
  try {
    // Refresh known candidate tokens in small batches. Also use latest profiles as a fallback discovery feed.
    const profileAddresses=[];
    const addresses=[...new Set([...latestTokens.keys(),...Object.keys(state.positions),...profileAddresses])].slice(-90);
    for(let i=0;i<addresses.length;i+=30) {
      const batch=addresses.slice(i,i+30);
      if(!batch.length)continue;
      let pairs=[];
      try{pairs=await fetchJson('https://api.dexscreener.com/tokens/v1/solana/'+batch.join(','))}catch(e){console.error('DEX batch:',e.message);continue}
      const byAddr=new Map();
      for(const p of pairs||[]) {
        if(p.chainId!=='solana'||!p.baseToken?.address||!p.priceUsd)continue;
        const addr=p.baseToken.address, liq=safeNum(p.liquidity?.usd);
        const old=byAddr.get(addr);
        if(!old||liq>safeNum(old.liquidity?.usd))byAddr.set(addr,p);
      }
      for(const [addr,p] of byAddr) {
        const old=latestTokens.get(addr)||{address:addr,name:p.baseToken.name,symbol:p.baseToken.symbol,createdAt:Date.now(),source:'DEX Screener profile'};
        const token={...old,name:p.baseToken.name||old.name,symbol:p.baseToken.symbol||old.symbol,priceUsd:safeNum(p.priceUsd),liquidityUsd:safeNum(p.liquidity?.usd),volume24h:safeNum(p.volume?.h24),buys24h:safeNum(p.txns?.h24?.buys),sells24h:safeNum(p.txns?.h24?.sells),change24h:safeNum(p.priceChange?.h24),url:p.url||old.url,status:'market data'};
        token.score=scoreToken(token);
        latestTokens.set(addr,token);
        const pos=state.positions[addr]; if(pos) pos.currentPrice=token.priceUsd;
        if(pos) {
          const change=(token.priceUsd-pos.entryPrice)/pos.entryPrice*100;
          if(change<=-STOP_LOSS_PCT) sellPaper(addr,'Stop-loss '+change.toFixed(1)+'%');
          else if(change>=TAKE_PROFIT_PCT) sellPaper(addr,'Take-profit '+change.toFixed(1)+'%');
        } else if(eligible(token)) buyPaper(token);
      }
    }
    lastMarketPollAt=new Date().toISOString();
    persist(); broadcast('state');
  } catch(e) {
    console.error('Market polling failed:',e.message);
    broadcast('error',{message:'DEX Screener feed temporarily unavailable: '+e.message});
  }
}
app.get('/api/health',(req,res)=>res.json({ok:true,feedStatus,uptimeSeconds:Math.round(process.uptime()),apiKeyConfigured:!!API_KEY,positions:Object.keys(state.positions).length,lastMarketPollAt,lastFeedMessageAt}));
app.get('/api/state',(req,res)=>res.json(publicState()));
app.post('/api/auto',(req,res)=>{
  if(typeof req.body.enabled!=='boolean')return res.status(400).json({error:'enabled boolean required'});
  autoEnabled=req.body.enabled; state.autoEnabled=autoEnabled;persist();broadcast('state');res.json({ok:true,autoEnabled});
});
app.post('/api/positions/:address/sell',(req,res)=>{
  const addr=req.params.address;if(!state.positions[addr])return res.status(404).json({error:'No open paper position'});
  sellPaper(addr,'Manual paper sell');persist();broadcast('state');res.json({ok:true});
});
app.post('/api/reset',(req,res)=>{
  if(req.body.confirm!=='RESET_PAPER_ONLY')return res.status(400).json({error:'Send confirm=RESET_PAPER_ONLY'});
  state={cash:START_CASH,initialCash:START_CASH,positions:{},history:[],seenMints:[],autoEnabled:false};
  seenMints=new Set();autoEnabled=false;persist();broadcast('state');res.json({ok:true});
});
wss.on('connection',ws=>{
  clients.add(ws);ws.send(JSON.stringify({type:'state',data:publicState()}));
  ws.on('close',()=>clients.delete(ws));
  ws.on('error',()=>clients.delete(ws));
});
state.autoEnabled = state.autoEnabled !== false;
autoEnabled=state.autoEnabled;
server.listen(PORT,'0.0.0.0',()=>{
  console.log(`FOMO Lab v3 listening on ${PORT}; persistent file ${DATA_FILE}`);
  connectPumpPortal();
  pollMarket();
  marketTimer=setInterval(pollMarket,60000);
});
process.on('SIGTERM',()=>{clearInterval(marketTimer);clearTimeout(reconnectTimer);try{pumpWs?.close()}catch{}persist();server.close(()=>process.exit(0))});
