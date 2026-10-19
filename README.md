# FOMO Lab v3 — Live discovery + paper trading

## What this is
- Node.js backend keeps running while the hosting service is running.
- PumpPortal WebSocket `subscribeNewToken` receives newly created Pump.fun tokens.
- DEX Screener public API supplies price/liquidity/volume data. Backend polls every 5 seconds.
- Auto paper strategy only simulates trades; it never signs or sends blockchain transactions.
- State is persisted to `DATA_FILE`. On Render, mount a persistent disk at `/var/data` and set `DATA_FILE=/var/data/fomo-state.json`.

## External services / keys
1. **PumpPortal**: `PUMPPORTAL_API_KEY` (get one from PumpPortal's official site; see https://pumpportal.fun/data-api/real-time/). Only new-token subscription is used. This app does not ask for a wallet private key and does not use a wallet.
2. **DEX Screener API**: public API used for market snapshots. No key is configured by this prototype; observe the provider's current rate limits/terms.
3. **Render**: suggested Node.js host. A persistent disk is needed for state to survive restarts/deploys. Always-on service availability may require a paid plan; free services can sleep.

## Strategy defaults (not proven profitable)
- Initial virtual cash: $1,000
- Virtual size: $50
- Max open positions: 4
- Entry score >= 65, liquidity >= $15k, 24h volume >= $20k, >= 40 24h transactions
- Skip tokens older than 15 minutes at the time they are first evaluated, 24h change > +35% or < -15%
- Stop loss -12%, take profit +25%
- 1% adverse entry and exit execution buffer to model costs/slippage
- New-token event alone is never enough to buy; market data must be available and pass filters.
- Current price checks use periodic DEX Screener snapshots, so stop/target fills are approximate and may be delayed.

## Deploy using GitHub from iPhone/iPad
1. Download and unzip `fomo_lab_v3.zip`.
2. In Safari, create a GitHub repository at https://github.com/new and upload all files/folders from the unzipped project. Ensure `server.js`, `package.json`, `render.yaml`, and `public/index.html` are at the repository root (not nested one extra folder).
3. Open https://dashboard.render.com/ and create a new **Web Service** from that repository.
4. Build command: `npm install`; Start command: `npm start`; runtime Node.
5. Add environment variable `PUMPPORTAL_API_KEY` in Render's Environment tab. Keep it secret; never put it in `public/index.html` or a public repository. Do not enter a wallet private key.
6. Create a persistent disk mounted at `/var/data` (1 GB) and set `DATA_FILE=/var/data/fomo-state.json`. `render.yaml` describes this but verify disk/service plan in dashboard.
7. Deploy. Open the Render service URL. `/api/health` should return JSON with `ok:true`. The dashboard should show `LIVE FEED` after the WebSocket connects.
8. Press **Auto-Trading starten** in the dashboard to enable simulated entries. The backend evaluates candidates automatically while the service is running.
9. Add your Netlify domain as a custom frontend only if desired; easiest is to use the Render URL, which serves the dashboard and backend from the same origin.

## Important limitations
- PumpPortal feed is a discovery feed, not a guarantee of complete Solana token coverage.
- The prototype relies on DEX Screener to validate price/liquidity and may miss extremely new tokens until they appear in market data.
- Liquidity/volume/transaction count are not reliable proof against scams, honeypots, wash trading, or inability to sell.
- This app does not buy/sell real crypto, connect a wallet, store seed phrases, or sign transactions.
- In-memory WebSocket candidate state resets on restart, but persisted portfolio/trade history is saved. `seenMints` is persisted to avoid repeatedly buying the same token.
- Do not treat simulated results as proof of live profitability.
