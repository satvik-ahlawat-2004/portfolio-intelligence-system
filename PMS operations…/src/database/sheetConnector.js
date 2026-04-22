import { CONFIG } from '../../config.js';
import { LOCAL_DB } from './local_db_static.js';

export class SheetConnector {
    constructor(sheetId, apiKey) {
        this.sheetId = sheetId;
        this.apiKey = apiKey;
        this.baseUrl = `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values`;
        
        // Caching
        this.rawDataCache = null;
    }

    async _fetchRawData() {
        if (this.rawDataCache) return this.rawDataCache;

        if (CONFIG.USE_LOCAL_DB) {
            this.rawDataCache = {
                clients: this._arrayToObjects(LOCAL_DB.clients),
                kyc_records: this._arrayToObjects(LOCAL_DB.kyc_records),
                instruments: this._arrayToObjects(LOCAL_DB.instruments),
                transactions: this._arrayToObjects(LOCAL_DB.transactions)
            };
        } else {
            // Fetch live from Google Sheets
            this.rawDataCache = {
                clients: await this._fetchSheet('clients'),
                kyc_records: await this._fetchSheet('kyc_records'),
                instruments: await this._fetchSheet('instruments'),
                transactions: await this._fetchSheet('transactions')
            };
        }
        
        return this.rawDataCache;
    }

    _arrayToObjects(arr) {
        if (!arr || arr.length < 2) return [];
        const headers = arr[0];
        const rows = arr.slice(1);
        return rows.map(row => 
            Object.fromEntries(headers.map((h, i) => [h, row[i]]))
        );
    }

    async getClients() {
        const data = await this._fetchRawData();
        const clients = data.clients || [];
        const kyc = data.kyc_records || [];
        const txns = data.transactions || [];

        return clients.map(c => {
            const client_id = c.client_id || c.Client_ID;
            const full_name = c.full_name || c.Client_Name;
            const risk_profile = c.risk_profile || c.Risk_Profile;
            const status = c.status || c.Status || 'Active';
            
            const k = kyc.find(r => (r.client_id || r.Client_ID) === client_id) || {};
            
            // Calculate total investment (sum of all BUY transactions)
            const clientTxns = txns.filter(t => (t.client_id || t.Client_ID) === client_id && (t.txn_type || t.Transaction_Type) === 'BUY');
            const totalInvestment = clientTxns.reduce((sum, t) => {
                const qty = parseFloat(t.quantity || t.Quantity || 0);
                const price = parseFloat(t.price || t.Price || 0);
                return sum + (qty * price);
            }, 0);

            return {
                clientId: client_id,
                name: full_name,
                pan: k.pan || k.PAN || 'N/A',
                email: 'N/A',
                phone: 'N/A',
                investment: totalInvestment,
                riskProfile: risk_profile,
                
                // Legacy entries required by rendering
                Client_ID: client_id,
                Client_Name: full_name,
                Risk_Profile: risk_profile,
                Investment_Amount: totalInvestment,
                Status: status
            };
        });
    }

    async getHoldings() {
        const data = await this._fetchRawData();
        const txns = data.transactions || [];
        const instruments = data.instruments || [];
        
        const holdingsMap = {};
        txns.forEach(t => {
            const clientId = t.client_id || t.Client_ID;
            const instrumentId = t.instrument_id || t.Stock_Name;
            const key = `${clientId}-${instrumentId}`;
            
            if (!holdingsMap[key]) {
                const inst = instruments.find(i => (i.instrument_id || i.Stock_Name) === instrumentId) || {};
                holdingsMap[key] = {
                    holdingId: `H-${key}`,
                    clientId: clientId,
                    stock: instrumentId,
                    sector: inst.asset_class || inst.Sector || 'Equity',
                    quantity: 0,
                    totalCost: 0
                };
            }
            const qty = parseFloat(t.quantity || t.Quantity) || 0;
            const price = parseFloat(t.price || t.Price) || 0;
            const type = t.txn_type || t.Transaction_Type;
            
            if (type === 'BUY') {
                holdingsMap[key].quantity += qty;
                holdingsMap[key].totalCost += qty * price;
            } else if (type === 'SELL') {
                holdingsMap[key].quantity -= qty;
            }
        });

        return Object.values(holdingsMap)
            .filter(h => h.quantity > 0)
            .map(h => ({
                ...h,
                buyPrice: h.totalCost / h.quantity,
                // Legacy entries required by rendering
                Holding_ID: h.holdingId,
                Client_ID: h.clientId,
                Stock_Name: h.stock,
                Sector: h.sector,
                Quantity: h.quantity,
                Buy_Price: h.totalCost / h.quantity
            }));
    }

    async getTransactions() {
        const data = await this._fetchRawData();
        return (data.transactions || []).map(t => ({
            transactionId: t.txn_id || t.Transaction_ID,
            clientId: t.client_id || t.Client_ID,
            stock: t.instrument_id || t.Stock_Name,
            type: t.txn_type || t.Transaction_Type,
            quantity: parseFloat(t.quantity || t.Quantity),
            price: parseFloat(t.price || t.Price),
            date: t.trade_date || t.Transaction_Date,
            // Legacy entries required by rendering
            Transaction_ID: t.txn_id || t.Transaction_ID,
            Client_ID: t.client_id || t.Client_ID,
            Stock_Name: t.instrument_id || t.Stock_Name,
            Transaction_Type: t.txn_type || t.Transaction_Type,
            Quantity: parseFloat(t.quantity || t.Quantity),
            Price: parseFloat(t.price || t.Price),
            Transaction_Date: t.trade_date || t.Transaction_Date
        }));
    }

    async getCompliance() {
        const data = await this._fetchRawData();
        const kyc = data.kyc_records || [];
        const clients = data.clients || [];
        
        return kyc.map(k => {
            const clientId = k.client_id || k.Client_ID;
            const c = clients.find(cl => (cl.client_id || cl.Client_ID) === clientId) || {};
            const kycStatus = k.kyc_status || k.KYC_Status;
            const riskProfile = c.risk_profile || c.Risk_Profile;
            const expiry = k.kyc_expiry_date || k.KYC_Expiry_Date;
            
            return {
                clientId: clientId,
                kycStatus: kycStatus,
                fatcaStatus: 'Done',
                riskProfile: riskProfile,
                kycExpiry: expiry,
                // Legacy entries required by rendering
                Client_ID: clientId,
                KYC_Status: kycStatus,
                FATCA_Status: 'Done',
                Risk_Profile: riskProfile,
                KYC_Expiry_Date: expiry,
                Compliance_Status: kycStatus === 'Valid' ? 'Compliant' : 'Action Required'
            };
        });
    }

    async addClientToSheet(clientData) {
        if (!CONFIG.APPS_SCRIPT_WEBHOOK) {
            console.warn("No Apps Script Webhook URL found. Simulating save for frontend only.");
            throw new Error("Missing Webhook URL");
        }
        
        try {
            // We use POST to send the payload to the custom Google Apps Script bridge
            const response = await fetch(CONFIG.APPS_SCRIPT_WEBHOOK, {
                method: 'POST',
                // Using text/plain avoids CORS preflight failures on Apps Script
                headers: {
                    'Content-Type': 'text/plain;charset=utf-8',
                },
                body: JSON.stringify({
                    action: 'add',
                    sheet: 'clients',
                    row: [clientData.id, clientData.name, clientData.risk, 'Active']
                })
            });

            if (!response.ok) {
                throw new Error("Webhook rejected the request");
            }
            
            this.rawDataCache = null;
            return true;
        } catch(err) {
            console.error("Failed to write proxy to Google Sheets:", err);
            throw err;
        }
    }

    async removeClientFromSheet(clientId) {
        if (!CONFIG.APPS_SCRIPT_WEBHOOK) {
            console.warn("No Apps Script Webhook URL found. Simulating remove for frontend only.");
            throw new Error("Missing Webhook URL");
        }
        
        try {
            const response = await fetch(CONFIG.APPS_SCRIPT_WEBHOOK, {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain;charset=utf-8',
                },
                body: JSON.stringify({
                    action: 'remove',
                    sheet: 'clients',
                    clientId: clientId
                })
            });

            if (!response.ok) {
                throw new Error("Webhook rejected the remove request");
            }
            
            this.rawDataCache = null;
            return true;
        } catch(err) {
            console.error("Failed to delete from Google Sheets:", err);
            throw err;
        }
    }

    async _fetchSheet(sheetName) {
        try {
            const fetchTarget = sheetName.toLowerCase();
            const response = await fetch(`${this.baseUrl}/${fetchTarget}?key=${this.apiKey}`);
            if(!response.ok) throw new Error(`API Fetch failed for ${sheetName}`);
            const data = await response.json();
            if(!data.values || data.values.length === 0) return [];
            return this._arrayToObjects(data.values);
        } catch(err) {
            console.warn(`Fetch failed for ${sheetName}:`, err.message);
            return [];
        }
    }
}
