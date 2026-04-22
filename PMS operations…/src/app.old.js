document.addEventListener('DOMContentLoaded', () => {
    // Basic Navigation Logic
    const navLinks = document.querySelectorAll('.nav-links li');
    const sections = document.querySelectorAll('.content-section');

    navLinks.forEach(link => {
        link.addEventListener('click', () => {
            navLinks.forEach(l => l.classList.remove('active'));
            link.classList.add('active');
            
            const target = link.dataset.target;
            sections.forEach(sec => {
                sec.classList.remove('active');
                if(sec.id === target) {
                    sec.classList.add('active');
                }
            });
        });
    });

    // Formatting utilities
    const formatCurrency = (amount) => {
        return new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(amount);
    };

    const formatPercent = (val) => {
        return val.toFixed(2) + '%';
    };

    // Calculate Portfolio Values (Dynamic update logic)
    const calculatePortfolioValues = () => {
        DB.Holdings.forEach(holding => {
            holding.Current_Value = holding.Quantity * holding.Current_Price;
            holding.Investment_Value = holding.Quantity * holding.Buy_Price;
        });
    };

    // Calculate AUM
    const calculateAUM = () => {
        let totalAUM = 0;
        DB.Holdings.forEach(h => { totalAUM += h.Current_Value; });
        return totalAUM;
    };

    // Calculate Performance per Client
    const calculatePerformance = () => {
        DB.Performance = [];
        DB.Clients.forEach(client => {
            let clientHoldings = DB.Holdings.filter(h => h.Client_ID === client.Client_ID);
            let currentVal = clientHoldings.reduce((sum, h) => sum + h.Current_Value, 0);
            
            if (currentVal > 0) {
                let initialInv = client.Investment_Amount;
                let profitLoss = currentVal - initialInv;
                let returnPct = (profitLoss / initialInv) * 100;
                let outperformance = returnPct - CONFIG.Benchmark_Return;
                
                DB.Performance.push({
                    Client_ID: client.Client_ID,
                    Initial_Investment: initialInv,
                    Current_Value: currentVal,
                    Profit_Loss: profitLoss,
                    Return_Percentage: returnPct,
                    Benchmark_Return: CONFIG.Benchmark_Return,
                    Outperformance: outperformance
                });
            }
        });
    };

    // INITIALIZATION & CALCULATION RUN
    calculatePortfolioValues();
    calculatePerformance();
    const currentAUM = calculateAUM();
    
    // DASHBOARD RENDERING
    document.getElementById('total-aum-val').innerText = formatCurrency(currentAUM);
    document.getElementById('total-clients-val').innerText = DB.Clients.length;
    
    // Charts Init
    const renderCharts = () => {
        // Sector Allocation
        const sectorMap = {};
        DB.Holdings.forEach(h => {
            sectorMap[h.Sector] = (sectorMap[h.Sector] || 0) + h.Current_Value;
        });

        new Chart(document.getElementById('sectorAllocationChart'), {
            type: 'doughnut',
            data: {
                labels: Object.keys(sectorMap),
                datasets: [{
                    data: Object.values(sectorMap),
                    backgroundColor: ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: { position: 'bottom', labels: { color: '#f8fafc' } }
                }
            }
        });

        // Top Clients
        const clientVal = DB.Performance.sort((a,b) => b.Current_Value - a.Current_Value).slice(0, 10);
        new Chart(document.getElementById('topClientsChart'), {
            type: 'bar',
            data: {
                labels: clientVal.map(c => c.Client_ID),
                datasets: [{
                    label: 'Portfolio Value',
                    data: clientVal.map(c => c.Current_Value),
                    backgroundColor: '#3b82f6'
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: { ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
                    x: { ticks: { color: '#94a3b8' }, grid: { display: false } }
                },
                plugins: {
                    legend: { display: false }
                }
            }
        });

        // Performance Chart (Portfolio vs Benchmark)
        new Chart(document.getElementById('benchmarkChart'), {
            type: 'bar',
            data: {
                labels: DB.Performance.map(p => p.Client_ID),
                datasets: [
                    {
                        label: 'Client Return %',
                        data: DB.Performance.map(p => p.Return_Percentage),
                        backgroundColor: '#10b981'
                    },
                    {
                        label: 'Benchmark Return %',
                        data: DB.Performance.map(p => p.Benchmark_Return),
                        backgroundColor: '#f59e0b'
                    }
                ]
            },
            options: {
                responsive: true,
                scales: {
                    y: { ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
                    x: { ticks: { color: '#94a3b8' }, grid: { display: false } }
                },
                plugins: {
                    legend: { position: 'bottom', labels: { color: '#f8fafc' } }
                }
            }
        });
    };
    renderCharts();

    // RENDER TABLES
    const renderTable = (tbodyId, dataArray, rowRenderer) => {
        const tbody = document.getElementById(tbodyId);
        if(!tbody) return;
        tbody.innerHTML = '';
        dataArray.forEach(item => {
            tbody.innerHTML += rowRenderer(item);
        });
    };

    // Client Management
    renderTable('clients-table-body', DB.Clients, c => `
        <tr>
            <td>${c.Client_ID}</td>
            <td>${c.Client_Name}</td>
            <td>${c.PAN}</td>
            <td>${c.Risk_Profile}</td>
            <td><span style="color: ${c.Status === 'Active' ? '#10b981' : '#ef4444'}">${c.Status}</span></td>
        </tr>
    `);

    // Holdings
    renderTable('holdings-table-body', DB.Holdings, h => `
        <tr>
            <td>${h.Client_ID}</td>
            <td>${h.Stock_Name}</td>
            <td>${h.Sector}</td>
            <td>${h.Quantity}</td>
            <td>${formatCurrency(h.Current_Value)}</td>
        </tr>
    `);

    // Transactions
    renderTable('transactions-table-body', DB.Transactions, t => `
        <tr>
            <td>${t.Transaction_ID}</td>
            <td>${t.Client_ID}</td>
            <td>${t.Stock_Name}</td>
            <td style="color: ${t.Transaction_Type === 'BUY' ? '#10b981' : '#ef4444'}">${t.Transaction_Type}</td>
            <td>${t.Quantity}</td>
            <td>${t.Transaction_Date}</td>
        </tr>
    `);

    // Compliance
    let expiringCount = 0;
    const now = new Date();
    renderTable('compliance-table-body', DB.Compliance, c => {
        const expiry = new Date(c.KYC_Expiry_Date);
        const diffDays = (expiry - now) / (1000 * 60 * 60 * 24);
        if(diffDays > 0 && diffDays <= 30) {
            expiringCount++;
        }
        
        let statusColor = c.Compliance_Status === 'Compliant' ? '#10b981' : '#f59e0b';
        return `
            <tr>
                <td>${c.Client_ID}</td>
                <td>${c.KYC_Status}</td>
                <td>${c.KYC_Expiry_Date}</td>
                <td style="color: ${statusColor}; font-weight: 500;">${c.Compliance_Status}</td>
            </tr>
        `;
    });
    document.getElementById('kyc-expiring-count').innerText = expiringCount;

    // Reports Generation Logic
    document.getElementById('generate-reports-btn').addEventListener('click', () => {
        DB.Reports = DB.Performance.map((p, idx) => ({
            Report_ID: 'REP' + (idx+1).toString().padStart(3, '0'),
            Client_ID: p.Client_ID,
            Report_Type: 'Monthly Performance',
            Report_Date: new Date().toISOString().split('T')[0],
            Portfolio_Value: p.Current_Value,
            Return_Percentage: p.Return_Percentage,
            Benchmark_Return: p.Benchmark_Return
        }));
        
        renderTable('reports-table-body', DB.Reports, r => `
            <tr>
                <td>${r.Report_ID}</td>
                <td>${r.Client_ID}</td>
                <td>${r.Report_Type}</td>
                <td>${formatCurrency(r.Portfolio_Value)}</td>
                <td style="color: ${r.Return_Percentage >= 0 ? '#10b981' : '#ef4444'}">
                    ${formatPercent(r.Return_Percentage)}
                </td>
            </tr>
        `);
        alert('Monthly client reports generated successfully!');
    });

    // Alert Handling (Simulated)
    const checkAlerts = () => {
        DB.Performance.forEach(p => {
            if (p.Return_Percentage < -5.0) {
                console.warn(`ALERT: Portfolio for ${p.Client_ID} dropped by > 5% (${formatPercent(p.Return_Percentage)})`);
            }
        });
    };
    checkAlerts();
});
