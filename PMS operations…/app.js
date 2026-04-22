import { CONFIG } from './config.js';
import { SheetConnector } from './src/database/sheetConnector.js';
import { StockPriceService } from './src/services/stockPriceService.js';
import { PortfolioEngine } from './src/services/portfolioEngine.js';
import { AumCalculator } from './src/services/aumCalculator.js';
import { ComplianceEngine } from './src/services/complianceEngine.js';
import { RiskAnalytics } from './src/services/riskAnalytics.js';
import { AiAdvisor } from './src/services/aiAdvisor.js';
import { AllocationChart } from './src/charts/allocationChart.js';
import { PerformanceChart } from './src/charts/performanceChart.js';
import { ReportGenerator } from './src/reports/reportGenerator.js';

class App {
    constructor() {
        this.sheetConnector = new SheetConnector(CONFIG.SHEET_ID, CONFIG.SHEETS_API_KEY);
        this.stockPriceService = new StockPriceService(CONFIG.STOCK_API_KEY);
        this.portfolioEngine = new PortfolioEngine(this.sheetConnector, this.stockPriceService);
        this.aumCalculator = new AumCalculator();
        this.complianceEngine = new ComplianceEngine();
        this.riskAnalytics = new RiskAnalytics();
        this.aiAdvisor = new AiAdvisor();
        this.reportGenerator = new ReportGenerator();
        
        this.data = { clients: [], holdings: [], transactions: [], compliance: [] };
        this.marketData = [];
        this.portfolios = [];
        this.isSyncing = false;
        
        this.allocationChart = new AllocationChart('sectorAllocationChart');
        this.performanceChart = new PerformanceChart('benchmarkChart');
        
        this.init();
    }

    async init() {
        this.setupNavigation();
        this.setupReportButton();
        
        console.log('Fetching Initial Data...');
        await this.loadData();
        await this.processMarketAndPortfolios();
        
        this.renderUI();
        
        // Auto Sync with throttling lock
        setInterval(() => this.backgroundSync(), CONFIG.POLL_INTERVAL_MS);
    }
    
    async loadData() {
        try {
            this.data.clients = await this.sheetConnector.getClients();
            this.data.holdings = await this.sheetConnector.getHoldings();
            this.data.transactions = await this.sheetConnector.getTransactions();
            this.data.compliance = await this.sheetConnector.getCompliance();
        } catch(err) {
            console.error('Error loading initial data: ', err);
        }
    }

    async processMarketAndPortfolios() {
        try {
            // Fetch Live Market Data Asynchronously
            this.marketData = await this.stockPriceService.fetchMarketData();
            
            // Synchronize and Update Portfolios using the Engine
            this.portfolios = await this.portfolioEngine.synchronize();
            this.data.holdings = this.portfolioEngine.holdings;
            this.data.clients = this.portfolioEngine.clients || this.data.clients;
            
            this.totalAum = this.aumCalculator.calculateTotalAum(this.portfolios);
            
            // Risk & Compliance calculations
            this.riskMetrics = this.riskAnalytics.calculatePortfolioRisk(this.portfolios);
            const { alerts, updatedRecords } = this.complianceEngine.evaluateCompliance(this.data.compliance);
            this.data.compliance = updatedRecords;
            
            // AI Advisor Analysis
            this.aiAnalysis = this.aiAdvisor.analyze(this.portfolios, this.marketData, this.data.holdings, this.riskMetrics, CONFIG.BENCHMARK_RETURN);

            // Dynamic Alerts System
            this.alerts = [...alerts, ...this.aiAnalysis.warnings];
            this.evaluateDynamicAlerts();
            
            // Failsafe Connectivity Warning
            if (!CONFIG.STOCK_API_KEY || CONFIG.STOCK_API_KEY.includes('YOUR_')) {
                 if(!this.alerts.find(a => a.type === 'API_FAILSAFE')) {
                     this.alerts.push({ type: 'API_FAILSAFE', message: 'API Network offline or exhausted. Operating safely using local synthetically modeled caches.' });
                 }
            }
            
            // Empty Dataset Warning
            if (this.data.clients.length === 0 || this.data.holdings.length === 0) {
                 if(!this.alerts.find(a => a.type === 'EMPTY_WARNING')) {
                     this.alerts.push({ type: 'EMPTY_WARNING', message: '⚠ Google Sheets connected but dataset is empty.' });
                 }
            }
            
        } catch(err) {
            console.error("Critical error during processing: ", err);
        }
    }

    evaluateDynamicAlerts() {
        this.portfolios.forEach(p => {
            if (p.Return_Percentage < -5.0 && !this.alerts.find(a => a.type==='DRAWDOWN' && a.message.includes(p.Client_ID))) {
                this.alerts.push({ type: 'PORTFOLIO_DROP', message: `Critical drop: ${p.Client_Name} portfolio down ${p.Return_Percentage.toFixed(2)}%` });
            }
        });
        
        this.marketData.forEach(m => {
            if (m.percentage_change < -5.0 && !m.is_index) {
                this.alerts.push({ type: 'STOCK_CRASH', message: `Market Warning: ${m.name} down ${m.percentage_change.toFixed(2)}% today` });
            }
            if (m.volume > (m.basePrice ? m.basePrice * 100 : 2000000)) { 
                this.alerts.push({ type: 'ABNORMAL_VOL', message: `Abnormal Volume detected for ${m.name}` });
            }
        });
    }

    async backgroundSync() {
        if(this.isSyncing) return; // Prevent overlapping execution
        this.isSyncing = true;
        
        console.log('Running automated async market sync...');
        await this.processMarketAndPortfolios();
        this.renderUI();
        
        this.isSyncing = false;
    }

    renderUI() {
        try {
            const formatCurrency = amount => {
                if (typeof amount !== 'number' || isNaN(amount)) return '₹0';
                return new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(amount);
            };
            const formatPercent = val => {
                if (typeof val !== 'number' || isNaN(val)) return '0.00%';
                return val.toFixed(2) + '%';
            };
            const pctColor = val => val >= 0 ? 'var(--accent)' : 'var(--danger)';

            // Top Dashboard KPI Grid Generation
            const topPerformer = this.portfolioEngine.getTopPerformer();
            const topStockName = topPerformer && topPerformer.Stock_Name ? topPerformer.Stock_Name : 'No Data';
            
            const marketStatus = this.stockPriceService.getMarketStatus();
        const marketOpen = marketStatus === 'Open';
        
        const healthScore = this.aiAnalysis ? this.aiAnalysis.healthScore : 0;
        const healthColor = healthScore > 80 ? 'var(--accent)' : (healthScore > 50 ? 'var(--warning)' : 'var(--danger)');

        const kpiGrid = document.getElementById('dashboardKPIGrid');
        if(kpiGrid) {
            kpiGrid.innerHTML = `
                <div class="card">
                    <h3>Total AUM</h3>
                    <p class="highlight">${this.totalAum ? formatCurrency(this.totalAum) : '₹0'}</p>
                    <p class="subtitle">Live Sync</p>
                </div>
                <div class="card">
                    <h3>Total Clients</h3>
                    <p class="highlight">${this.data.clients.length}</p>
                    <p class="subtitle">Active Portfolios</p>
                </div>
                <div class="card">
                    <h3>Portfolio Health</h3>
                    <p class="highlight" style="color: ${healthColor}">${healthScore}/100</p>
                    <p class="subtitle">AI Score</p>
                </div>
                <div class="card">
                    <h3>Total Holdings</h3>
                    <p class="highlight">${this.data.holdings.length}</p>
                    <p class="subtitle">Unique Assets</p>
                </div>
                <div class="card">
                    <h3>Top Performing Stock</h3>
                    <p class="highlight positive" style="font-size:1.5rem">${topStockName}</p>
                    <p class="subtitle">Daily Contributor</p>
                </div>
                <div class="card">
                    <h3>Market Status</h3>
                    <p class="highlight ${marketOpen ? 'positive' : 'warning'}">${marketOpen ? 'Open' : 'Closed'}</p>
                    <p class="subtitle">Live Execution</p>
                </div>
            `;
        }

        // Alerts Banner (Flash critical notifications)
        const alertsPanel = document.getElementById('alert-notifications');
        if(alertsPanel && this.alerts) {
            alertsPanel.innerHTML = '';
            this.alerts.slice(0, 3).forEach(alert => {
                 alertsPanel.innerHTML += `<div class="alert-box ${alert.type === 'DRAWDOWN' || alert.type === 'CONCENTRATION_RISK' ? 'warning' : 'warning'}" style="padding: 12px; font-size: 14px;">⚡ ${alert.message}</div>`;
            });
        }

        // AI Advisor Rendering
        if(this.aiAnalysis) {
            const aiRing = document.getElementById('ai-health-ring');
            if(aiRing) {
                aiRing.innerText = this.aiAnalysis.healthScore;
                aiRing.style.borderColor = healthColor;
                aiRing.style.color = healthColor;
            }

            const pAnalysis = this.aiAnalysis.performanceAnalysis;
            const topHoldEl = document.getElementById('ai-top-holding');
            if(topHoldEl) topHoldEl.innerText = pAnalysis.topHolding ? `${pAnalysis.topHolding.Stock_Name} (${formatPercent(pAnalysis.topHolding.Stock_Percentage_Change)})` : '-';
            
            const worstHoldEl = document.getElementById('ai-worst-holding');
            if(worstHoldEl) worstHoldEl.innerText = pAnalysis.worstHolding ? `${pAnalysis.worstHolding.Stock_Name} (${formatPercent(pAnalysis.worstHolding.Stock_Percentage_Change)})` : '-';
            
            const aiTopContr = document.getElementById('ai-top-contributor');
            if(aiTopContr) aiTopContr.innerText = pAnalysis.largestContributor ? `${pAnalysis.largestContributor.Stock_Name}` : '-';

            const aiInsightsBlock = document.getElementById('ai-insights-container');
            if(aiInsightsBlock) {
                aiInsightsBlock.innerHTML = '';
                this.aiAnalysis.insights.forEach(ins => {
                    aiInsightsBlock.innerHTML += `
                       <div class="ai-card">
                           <span class="ai-pill">Insight</span>
                           <p style="font-size: 0.9rem; line-height: 1.5;">${ins}</p>
                       </div>
                    `;
                });
                this.aiAnalysis.rebalancing.forEach(reb => {
                    aiInsightsBlock.innerHTML += `
                       <div class="ai-card">
                           <span class="ai-pill" style="background: rgba(16, 185, 129, 0.2); color: var(--accent);">Rebalancing Suggestion</span>
                           <p style="font-size: 0.9rem; line-height: 1.5;">${reb}</p>
                       </div>
                    `;
                });
            }

            const aiMarketBlock = document.getElementById('ai-market-insights-container');
            if(aiMarketBlock) {
                aiMarketBlock.innerHTML = '';
                this.aiAnalysis.marketInsights.forEach(mi => {
                     aiMarketBlock.innerHTML += `
                       <div class="ai-card">
                           <span class="ai-pill" style="background: rgba(245, 158, 11, 0.2); color: var(--warning);">Market Intelligence</span>
                           <p style="font-size: 0.9rem; line-height: 1.5;">${mi}</p>
                       </div>
                    `;
                });
            }
        }

        // Market Insights Dynamic Grid
        const marketGrid = document.getElementById('marketOverview');
        if(marketGrid) {
            const nifty = this.marketData.find(m => m.symbol === 'NIFTY50') || { latest_price: 0, percentage_change: 0 };
            const sensex = this.marketData.find(m => m.symbol === 'SENSEX') || { latest_price: 0, percentage_change: 0 };
            
            const stocksOnly = this.marketData.filter(m => !m.is_index).sort((a,b) => b.percentage_change - a.percentage_change);
            const topGainer = stocksOnly.length > 0 ? `${stocksOnly[0].symbol} (${formatPercent(stocksOnly[0].percentage_change)})` : 'No Data';
            const topLoser = stocksOnly.length > 0 ? `${stocksOnly[stocksOnly.length - 1].symbol} (${formatPercent(stocksOnly[stocksOnly.length - 1].percentage_change)})` : 'No Data';

            marketGrid.innerHTML = `
                <div class="card">
                    <h3>NIFTY 50</h3>
                    <p class="highlight">${nifty.latest_price.toFixed(2)}</p>
                    <p class="subtitle" style="color: ${pctColor(nifty.percentage_change)}; font-weight:600">${formatPercent(nifty.percentage_change)}</p>
                </div>
                <div class="card">
                    <h3>SENSEX</h3>
                    <p class="highlight">${sensex.latest_price.toFixed(2)}</p>
                    <p class="subtitle" style="color: ${pctColor(sensex.percentage_change)}; font-weight:600">${formatPercent(sensex.percentage_change)}</p>
                </div>
                <div class="card">
                    <h3>Top Gainer</h3>
                    <p style="font-weight:600; font-size:1.4rem; color:var(--accent); margin-top:8px">${topGainer}</p>
                </div>
                <div class="card">
                    <h3>Top Loser</h3>
                    <p style="font-weight:600; font-size:1.4rem; color:var(--danger); margin-top:8px">${topLoser}</p>
                </div>
            `;
        }
        let globalLargest = { name: 'None', val: 0 };
        this.data.holdings.forEach(h => {
            if(h.Holding_Value > globalLargest.val) { globalLargest = { name: h.stock || h.Stock_Name, val: h.Holding_Value }; }
        });

        const largestAllocEl = document.getElementById('insight-largest-allocation');
        if (largestAllocEl) largestAllocEl.innerText = `${globalLargest.name} (${formatCurrency(globalLargest.val)})`;
        
        if(this.portfolios.length > 0) {
            let sortedPorts = [...this.portfolios].sort((a,b) => b.Return_Percentage - a.Return_Percentage);
            const topPortEl = document.getElementById('insight-top-portfolio');
            if (topPortEl) topPortEl.innerText = `${sortedPorts[0].Client_ID} (${formatPercent(sortedPorts[0].Return_Percentage)})`;
        }
        if(this.riskMetrics && this.riskMetrics.length > 0) {
            let sortedRisk = [...this.riskMetrics].sort((a,b) => b.Volatility - a.Volatility);
            const worstPortEl = document.getElementById('insight-worst-portfolio');
            if (worstPortEl) worstPortEl.innerText = `${sortedRisk[0].Client_ID} (Vol: ${sortedRisk[0].Volatility.toFixed(2)})`;
        }
        
        // Charts
        if(this.data.holdings.length) this.allocationChart.render(this.data.holdings);
        if(this.portfolios.length) this.performanceChart.render(this.portfolios, CONFIG.BENCHMARK_RETURN);
        
        // Tables Rendering
        console.log("Rendering Tables... Clients count:", this.data.clients.length);
        
        this.renderTable('market-table-body', this.marketData.filter(m => !m.is_index), m => `
            <tr>
                <td><strong>${m.symbol}</strong></td>
                <td>${m.name}</td>
                <td>${m.latest_price.toFixed(2)}</td>
                <td style="color: ${pctColor(m.daily_change)}">${m.daily_change > 0 ? '+':''}${m.daily_change.toFixed(2)}</td>
                <td style="color: ${pctColor(m.percentage_change)}">${m.percentage_change > 0 ? '+':''}${m.percentage_change.toFixed(2)}%</td>
                <td>${m.volume.toLocaleString()}</td>
            </tr>
        `);

        this.renderTable('clients-table-body', this.data.clients, c => {
            console.log("Rendering client row:", c.Client_ID);
            // Fetch real-time Portfolio Value to match accurate AUM
            const port = this.portfolios.find(p => p.Client_ID === c.Client_ID);
            const accurateValue = port ? port.Portfolio_Value : 0;
            
            return `
            <tr>
                <td>${c.Client_ID}</td>
                <td>${c.Client_Name}</td>
                <td>${c.Risk_Profile}</td>
                <td style="font-weight: 600;">${formatCurrency(accurateValue)}</td>
                <td><span style="color: ${c.Status === 'Active' ? 'var(--accent)' : 'var(--text-muted)'}">${c.Status}</span></td>
            </tr>
        `;
        });
        console.log("Clients table render complete.");

        this.renderTable('holdings-table-body', this.data.holdings, h => `
            <tr>
                <td>${h.Client_ID}</td>
                <td>${h.Stock_Name}</td>
                <td>${h.Quantity}</td>
                <td>${formatCurrency(h.Holding_Value)}</td>
                <td style="color: ${pctColor(h.Stock_Percentage_Change)}">${h.Stock_Percentage_Change.toFixed(2)}%</td>
            </tr>
        `);

        this.renderTable('transactions-table-body', this.data.transactions, t => `
            <tr>
                <td>${t.Transaction_ID}</td>
                <td>${t.Client_ID}</td>
                <td>${t.Stock_Name}</td>
                <td style="color: ${t.Transaction_Type === 'BUY' ? 'var(--accent)' : 'var(--danger)'}">${t.Transaction_Type}</td>
                <td>${t.Quantity}</td>
                <td>${formatCurrency(t.Price)}</td>
            </tr>
        `);
        
        document.getElementById('kyc-expiring-count').innerText = this.data.compliance.filter(c => c.Compliance_Status === 'Action Required').length;
        
        this.renderTable('compliance-table-body', this.data.compliance, c => `
            <tr>
                <td>${c.Client_ID}</td>
                <td>${c.KYC_Status}</td>
                <td>${c.KYC_Expiry_Date}</td>
                <td style="color: ${c.Compliance_Status === 'Compliant' ? 'var(--accent)' : 'var(--warning)'}; font-weight:500;">
                    ${c.Compliance_Status}
                </td>
            </tr>
        `);
        } catch (e) {
            console.error("UI Rendering Error:", e);
        }
    }

    renderTable(tbodyId, dataArray, rowRenderer) {
        const tbody = document.getElementById(tbodyId);
        if(!tbody) return;
        tbody.innerHTML = '';
        if(!dataArray || !Array.isArray(dataArray) || dataArray.length === 0) {
            tbody.innerHTML = '<tr><td colspan="10" style="text-align:center; padding: 20px; color: var(--text-muted);">No operational data available</td></tr>';
            return;
        }
        dataArray.forEach(item => {
            tbody.innerHTML += rowRenderer(item);
        });
    }

    setupNavigation() {
        const sections = document.querySelectorAll('.content-section');
        const navLinks = document.querySelectorAll('.nav-links li');

        window.switchSection = (target) => {
            console.log("Global Switching to section:", target);
            
            navLinks.forEach(l => {
                l.classList.remove('active');
                if(l.dataset.target === target) l.classList.add('active');
            });
            
            sections.forEach(sec => {
                sec.classList.remove('active');
                if(sec.id === target) {
                    sec.classList.add('active');
                }
            });
        };

        window.openAddClientModal = () => {
            document.getElementById('add-client-modal').style.display = 'flex';
        };

        window.closeAddClientModal = () => {
            document.getElementById('add-client-modal').style.display = 'none';
        };

        window.openRemoveClientModal = () => {
            document.getElementById('remove-client-modal').style.display = 'flex';
        };

        window.closeRemoveClientModal = () => {
            document.getElementById('remove-client-modal').style.display = 'none';
        };

        const addClientForm = document.getElementById('add-client-form');
        if (addClientForm) {
            addClientForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                const id = document.getElementById('new-client-id').value;
                const name = document.getElementById('new-client-name').value;
                const risk = document.getElementById('new-client-risk').value;
                
                try {
                    await window.appInstance.sheetConnector.addClientToSheet({ id, name, risk });
                    alert('Client successfully dispatched to Google Sheets backend!');
                    window.closeAddClientModal();
                    
                    // Quick refresh
                    await window.appInstance.loadData();
                    await window.appInstance.processMarketAndPortfolios();
                    window.appInstance.renderUI();
                } catch(err) {
                    alert('Backend Authorization Required for Writes. (Fallback local append executed)');
                    // Fallback to local memory update
                    window.appInstance.data.clients.push({
                        Client_ID: id,
                        client_id: id,
                        Client_Name: name,
                        full_name: name,
                        Risk_Profile: risk,
                        risk_profile: risk,
                        Investment_Amount: 0,
                        Status: 'Active'
                    });
                    window.appInstance.renderUI();
                    window.closeAddClientModal();
                }
            });
        }

        const removeClientForm = document.getElementById('remove-client-form');
        if (removeClientForm) {
            removeClientForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                const id = document.getElementById('remove-client-id').value.trim();
                
                try {
                    await window.appInstance.sheetConnector.removeClientFromSheet(id);
                    alert(`Client ${id} removal dispatched to Google Sheets!`);
                    window.closeRemoveClientModal();
                    
                    // Quick refresh
                    await window.appInstance.loadData();
                    await window.appInstance.processMarketAndPortfolios();
                    window.appInstance.renderUI();
                } catch(err) {
                    alert('Backend Authorization Required for Writes. (Fallback local removal executed)');
                    // Fallback to local memory update
                    window.appInstance.data.clients = window.appInstance.data.clients.filter(
                        c => c.client_id !== id && c.Client_ID !== id
                    );
                    window.appInstance.renderUI();
                    window.closeRemoveClientModal();
                }
            });
        }

        const navContainer = document.querySelector('.nav-links');
        if (!navContainer) return;

        navContainer.addEventListener('click', (e) => {
            const link = e.target.closest('li');
            if (link) window.switchSection(link.dataset.target);
        });
    }
    
    setupReportButton() {
        document.getElementById('generate-reports-btn').addEventListener('click', () => {
            const reports = this.reportGenerator.generateMonthlyReport(this.portfolios, CONFIG.BENCHMARK_RETURN);
            const formatCurrency = (amount) => new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(amount);
            const formatPercent = (val) => val.toFixed(2) + '%';
            
            this.renderTable('reports-table-body', reports, r => `
                <tr>
                    <td>${r.Report_ID}</td>
                    <td>${r.Client_ID}</td>
                    <td>${formatCurrency(r.Portfolio_Value)}</td>
                    <td style="color: ${r.Return_Percentage >= 0 ? 'var(--accent)' : 'var(--danger)'}">
                        ${formatPercent(r.Return_Percentage)}
                    </td>
                    <td style="font-size: 0.85rem; color: var(--text-muted); max-width: 300px;">
                        ${r.Commentary}
                    </td>
                </tr>
            `);
            alert('Monthly client reports generated displaying automated commentary!');
        });
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.appInstance = new App();
});
