export class PortfolioEngine {
    constructor(sheetConnector, stockPriceService) {
        this.sheetConnector = sheetConnector;
        this.stockPriceService = stockPriceService;
        
        this.holdings = [];
        this.clients = [];
        this.portfolios = [];
    }

    async synchronize() {
        this.clients = await this.sheetConnector.getClients();
        const rawHoldings = await this.sheetConnector.getHoldings();
        const marketData = await this.stockPriceService.fetchMarketData();

        this.holdings = rawHoldings.map(holding => {
             const mData = marketData.find(m => m.symbol === holding.stock || m.symbol === holding.Stock_Name);
             const livePrice = mData ? mData.latest_price : holding.buyPrice;
             const pctChange = mData ? mData.percentage_change : 0;
             const quantity = holding.quantity || holding.Quantity;

             const holdingValue = quantity * livePrice;
             const investmentValue = quantity * (holding.buyPrice || holding.Buy_Price);
             
             return {
                 ...holding,
                 Stock_Name: holding.stock || holding.Stock_Name,
                 Live_Price: livePrice,
                 Holding_Value: holdingValue,
                 holdingValue: holdingValue,
                 Investment_Value: investmentValue,
                 Stock_Percentage_Change: pctChange
             };
        });

        this.portfolios = this.clients.map(client => {
            const clientId = client.clientId || client.Client_ID;
            const clientHoldings = this.holdings.filter(h => h.clientId === clientId || h.Client_ID === clientId);
            const portfolioValue = clientHoldings.reduce((sum, h) => sum + h.Holding_Value, 0);
            const investmentAmount = client.investment || client.Investment_Amount;
            
            const profitLoss = portfolioValue - investmentAmount;
            const returnPct = investmentAmount > 0 ? (profitLoss / investmentAmount) * 100 : 0;
            
            const holdingContributions = clientHoldings.map(h => {
                const weight = portfolioValue > 0 ? h.Holding_Value / portfolioValue : 0;
                const contribution = weight * returnPct;
                return {
                    Stock_Name: h.stock || h.Stock_Name,
                    Weight: weight,
                    Contribution: contribution,
                    Holding_Value: h.Holding_Value
                };
            }).sort((a,b) => b.Contribution - a.Contribution);

            return {
                clientId: clientId,
                Client_ID: clientId,
                Client_Name: client.name || client.Client_Name,
                portfolioValue: portfolioValue,
                Portfolio_Value: portfolioValue,
                Investment_Value: investmentAmount,
                Profit_Loss: profitLoss,
                Return_Percentage: returnPct,
                holdings: clientHoldings,
                Holding_Contributions: holdingContributions
            };
        });

        return this.portfolios;
    }

    getClientPortfolio(clientId) {
        if (!clientId) return this.portfolios;
        return this.portfolios.find(p => p.clientId === clientId || p.Client_ID === clientId);
    }

    getTopPerformer() {
        if (this.holdings.length === 0) return { Stock_Name: '-', holdingValue: 0 };
        const sorted = [...this.holdings].sort((a,b) => b.Stock_Percentage_Change - a.Stock_Percentage_Change);
        return sorted[0];
    }

    getPortfolioAllocation() {
        const allocation = {};
        this.holdings.forEach(h => {
             const sector = h.sector || h.Sector || 'Others';
             allocation[sector] = (allocation[sector] || 0) + h.Holding_Value;
        });
        
        const totalValue = this.holdings.reduce((sum, h) => sum + h.Holding_Value, 0);
        
        const percentages = {};
        for(const [sector, val] of Object.entries(allocation)) {
             percentages[sector] = totalValue > 0 ? (val / totalValue) * 100 : 0;
        }
        return percentages;
    }

    getTotalHoldings() {
        return this.holdings.length;
    }
}
