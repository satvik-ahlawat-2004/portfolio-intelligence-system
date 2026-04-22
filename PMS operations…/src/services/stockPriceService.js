import { CONFIG } from '../../config.js';

export class StockPriceService {
    constructor(apiKey) {
        this.apiKey = apiKey;
        this.baseUrl = 'https://www.alphavantage.co/query'; 
        this.cache = new Map();
        this.cacheTTL = 60 * 1000;
        this.localMarketData = null;

        this.marketDataFallback = {
            'RELIANCE': { name: 'Reliance Ind.', basePrice: 2950, volume: 1540200 },
            'TCS': { name: 'Tata Consultancy Svcs', basePrice: 3920, volume: 920500 },
            'HDFCBANK': { name: 'HDFC Bank', basePrice: 1630, volume: 3204000 },
            'INFY': { name: 'Infosys', basePrice: 1715, volume: 2100340 }
        };
        this.history = {}; 
    }

    async _fetchLocalMarketData() {
        if (this.localMarketData) return this.localMarketData;
        if (!CONFIG.USE_LOCAL_DB) return null;
        try {
            const response = await fetch(CONFIG.LOCAL_DB_PATH);
            const fullDb = await response.json();
            const prices = fullDb.market_prices;
            if (!prices || prices.length < 2) return null;
            
            const headers = prices[0];
            this.localMarketData = prices.slice(1).map(row => {
                const obj = {};
                headers.forEach((h, i) => obj[h] = row[i]);
                return obj;
            });
            return this.localMarketData;
        } catch (err) {
            return null;
        }
    }

    getMarketStatus() {
        const now = new Date();
        const hr = now.getHours();
        const min = now.getMinutes();
        const isWeekday = now.getDay() >= 1 && now.getDay() <= 5;
        const open = isWeekday && ((hr > 9 || (hr === 9 && min >= 15)) && (hr < 15 || (hr === 15 && min <= 30)));
        return open ? 'Open' : 'Closed';
    }

    async getStockPrice(symbol) {
        if (!symbol) return null;
        
        // Try local DB first if enabled
        const localPrices = await this._fetchLocalMarketData();
        if (localPrices) {
            const match = localPrices.find(p => p.instrument_id === symbol);
            if (match) {
                const price = parseFloat(match.close_price);
                return {
                    symbol: symbol,
                    price: price,
                    change: 0,
                    changePercent: 0,
                    volume: 0,
                    latest_price: price,
                    daily_change: 0,
                    percentage_change: 0,
                    name: symbol,
                    is_index: false
                };
            }
        }

        if (this.cache.has(symbol)) {
            const cachedResult = this.cache.get(symbol);
            if (Date.now() - cachedResult.timestamp < this.cacheTTL) return cachedResult.data;
        }

        try {
            if (!this.apiKey || this.apiKey === 'YOUR_STOCK_API_KEY' || this.apiKey.includes('DEMO')) throw new Error("No Key");
            const response = await fetch(`${this.baseUrl}?function=GLOBAL_QUOTE&symbol=${symbol}&apikey=${this.apiKey}`);
            const data = await response.json();
            if (data['Global Quote']) {
                const quote = data['Global Quote'];
                const res = {
                    symbol: symbol,
                    price: parseFloat(quote['05. price']),
                    latest_price: parseFloat(quote['05. price']),
                    percentage_change: parseFloat((quote['10. change percent'] || '0').replace('%', '')),
                    name: symbol
                };
                this.cache.set(symbol, { data: res, timestamp: Date.now() });
                return res;
            }
            throw new Error("Invalid");
        } catch(err) {
            const fb = this.marketDataFallback[symbol] || { name: symbol, basePrice: 1000, volume: 100000 };
            const tick = this._generateTick(symbol, fb);
            return { ...tick, price: tick.latest_price };
        }
    }

    async getBulkPrices(symbols) {
        const results = await Promise.all(symbols.map(s => this.getStockPrice(s)));
        const map = {};
        results.forEach(r => { if(r) map[r.symbol] = r.price; });
        return map;
    }

    async fetchMarketData() {
        const symbols = Object.keys(this.marketDataFallback);
        return Promise.all(symbols.map(s => this.getStockPrice(s)));
    }

    _generateTick(symbol, data) {
        let base = data.basePrice;
        let volatility = base * (Math.random() * 0.04 - 0.02);
        let price = base + volatility;
        return {
            symbol: symbol,
            name: data.name,
            latest_price: price,
            daily_change: volatility,
            percentage_change: (volatility / base) * 100,
            volume: data.volume
        };
    }
}
