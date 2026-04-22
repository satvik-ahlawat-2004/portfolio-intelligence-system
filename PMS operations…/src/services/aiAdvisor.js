export class AiAdvisor {
    constructor() {}

    analyze(portfolios, marketData, globalHoldings, riskMetrics, benchmarkReturn) {
        let globalPortfolioValue = portfolios.reduce((sum, p) => sum + p.Portfolio_Value, 0);

        // Calculate Sector Allocations
        const sectorAllocations = {};
        globalHoldings.forEach(h => {
            sectorAllocations[h.Sector] = (sectorAllocations[h.Sector] || 0) + h.Holding_Value;
        });

        const sectorPercentages = {};
        let highestSector = { name: '', pct: 0 };
        for (const [sector, value] of Object.entries(sectorAllocations)) {
            let pct = globalPortfolioValue > 0 ? (value / globalPortfolioValue) * 100 : 0;
            sectorPercentages[sector] = pct;
            if (pct > highestSector.pct) {
                highestSector = { name: sector, pct: pct };
            }
        }

        // Global Volatility mock
        const avgVolatility = riskMetrics.length > 0 ? Math.max(...riskMetrics.map(r => r.Volatility)) : 0;
        
        // 1. Portfolio Health Score
        let score = 100;
        if (highestSector.pct > 35) score -= 15;
        if (highestSector.pct > 50) score -= 10;
        if (avgVolatility > 8) score -= 15;
        if (portfolios.some(p => p.Return_Percentage < benchmarkReturn)) score -= 10;
        score = Math.max(0, Math.min(100, Math.round(score)));

        // 2. AI Investment Insights & Warnings & Rebalancing
        const insights = [];
        const warnings = [];
        const rebalancing = [];

        // Diversification & Sectors
        if (highestSector.pct > 35) {
            insights.push(`Portfolio diversification is low. ${highestSector.name} sector allocation exceeds ${Math.round(highestSector.pct)}%.`);
            warnings.push({ type: 'CONCENTRATION_RISK', message: `${highestSector.name} sector allocation exceeds 35% threshold.`});
            
            let reducePct = Math.round(highestSector.pct - 25); // Target 25%
            rebalancing.push(`Reduce exposure to ${highestSector.name} sector by ~${reducePct}% to improve diversification.`);
            
            let lowestSector = Object.keys(sectorPercentages).reduce((a, b) => sectorPercentages[a] < sectorPercentages[b] ? a : b);
            rebalancing.push(`Consider increasing allocation to ${lowestSector} sector.`);
        } else {
            insights.push(`Portfolio is well-diversified across sectors.`);
        }

        // Volatility Check
        if (avgVolatility > 8) {
             insights.push(`Portfolio volatility is currently running higher than the historical benchmark.`);
             warnings.push({ type: 'HIGH_VOLATILITY', message: `Portfolio volatility is elevated (${avgVolatility.toFixed(2)}).`});
        }

        // Out/Under Performance Check
        const outperformingPorts = portfolios.filter(p => p.Return_Percentage > benchmarkReturn);
        if (outperformingPorts.length > 0) {
            let avgOut = outperformingPorts.reduce((s, p) => s + (p.Return_Percentage - benchmarkReturn), 0) / outperformingPorts.length;
            insights.push(`A segment of client portfolios is outperforming the market benchmark by an average of ${avgOut.toFixed(2)}%.`);
        }

        portfolios.forEach(p => {
             if(p.Return_Percentage < -10) {
                  warnings.push({ type: 'DRAWDOWN', message: `Severe Drawdown: ${p.Client_ID} portfolio has dropped by >10%.`});
             }
        });

        // 3. Market Insight Generation
        let marketInsights = [];
        const nifty = marketData.find(m => m.symbol === 'NIFTY50');
        if (nifty && nifty.percentage_change !== 0) {
             let dir = nifty.percentage_change > 0 ? 'upward' : 'downward';
             marketInsights.push(`NIFTY index is trending ${dir} compared to the previous trading session (Change: ${nifty.percentage_change.toFixed(2)}%).`);
        }
        
        let highVolStock = marketData.filter(m => !m.is_index && Math.abs(m.percentage_change) > 3);
        if (highVolStock.length > 0) {
             marketInsights.push(`Market volatility increased today driven by strong movements in ${highVolStock[0].name}.`);
        }

        // 4. Performance Analysis (Holdings)
        let sortedHoldings = [...globalHoldings].filter(h => h.Investment_Value > 0).sort((a,b) => b.Stock_Percentage_Change - a.Stock_Percentage_Change);
        let topHolding = sortedHoldings.length > 0 ? sortedHoldings[0] : null;
        let worstHolding = sortedHoldings.length > 0 ? sortedHoldings[sortedHoldings.length-1] : null;

        const performanceAnalysis = {
            topHolding,
            worstHolding,
            largestContributor: topHolding
        };

        return {
            healthScore: score,
            insights,
            warnings,
            rebalancing,
            marketInsights,
            performanceAnalysis
        };
    }
}
