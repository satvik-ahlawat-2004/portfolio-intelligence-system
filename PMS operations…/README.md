# PMS AI Portfolio Dashboard

An enterprise-grade, browser-native FinTech dashboard designed for Portfolio Management Services (PMS). It integrates real-time mock market intelligence, programmatic API-fallback architectures, and a fully functional embedded AI Portfolio Advisory logic.

## System Architecture

This application operates completely asynchronously without requiring complex Node.js build steps, making it incredibly resilient and lightning-fast to host. The system uses a modernized **ES Modules** JavaScript structure.

```
/
├── index.html            # Main Entry Point
├── styles.css            # Bespoke FinTech Theme
├── app.js                # Core Initialization and UI Bindings
├── config.js             # Environment Config & API Failsafes
└── src/
    ├── database/
    │   └── sheetConnector.js     # Google Sheets connector (with Fallbacks)
    ├── services/
    │   ├── aiAdvisor.js          # AI Health Scoring & Portfolio Rebalancing Insights
    │   ├── aumCalculator.js      # Global AUM mathematics
    │   ├── complianceEngine.js   # Dynamic KYC/FATCA Alert Generation
    │   ├── portfolioEngine.js    # Asset allocation & contribution weighting
    │   ├── riskAnalytics.js      # Portfolio Volatility computations
    │   └── stockPriceService.js  # Live Market Intelligence polling
    ├── charts/
    │   ├── allocationChart.js    # Chart.js Donut allocations
    │   └── performanceChart.js   # Chart.js Bar distributions
    └── reports/
        └── reportGenerator.js    # Fully automated algorithmic reporting 
```

## Production Optimization & API Failsafes

1. **Rendering Overhead Minimization**: 
   UI renders trigger exclusively upon asynchronous lock resolution. Specifically, `app.js` runs a `backgroundSync` check leveraging `this.isSyncing = true` locks to eradicate overlapping intervals or state corruption during slow market polls.
2. **Dynamic Error Failsafes**: 
   Built-in `try/catch` fallbacks ensure that if the AlphaVantage / Google Sheets APIs rate limit or timeout, the robust mock engines step in immediately preventing 0-byte structural dashboard crashes.
3. **Module Loading**:
   Utilizes pure browser `type="module"` paths, natively optimizing standard fetching.

## How to Deploy (Vercel, Netlify, GitHub Pages)

Because this structure was strictly compiled to operate staticaly without arbitrary build steps (`npm run build`), deploying is instant.

1. Create a repository bridging this base folder.
2. Link the repository to your host platform (e.g. Vercel, Netlify).
3. The platform will read `index.html` located in root and instantly deploy the functioning web-app asynchronously!

*Note: Environment variables are located in `config.js` for modular insertion across deployment.*

## Local Testing

You can use python or npx to serve the root directory over HTTP instantly.

```bash
npx serve . 
# OR
python -m http.server 8000
```
Open your browser resolving to localhost, and you will see the PMS AI Dashboard fully operational.
