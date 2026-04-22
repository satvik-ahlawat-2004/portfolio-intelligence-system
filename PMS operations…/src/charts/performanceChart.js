export class PerformanceChart {
    constructor(canvasId) {
        this.ctx = document.getElementById(canvasId);
        this.chart = null;
    }

    render(portfolios, benchmarkReturn) {
        if (this.chart) this.chart.destroy();

        this.chart = new Chart(this.ctx, {
            type: 'bar',
            data: {
                labels: portfolios.map(p => p.Client_ID),
                datasets: [
                    {
                        label: 'Client Return %',
                        data: portfolios.map(p => p.Return_Percentage),
                        backgroundColor: '#10b981',
                        borderRadius: 4
                    },
                    {
                        label: 'Benchmark Return %',
                        data: portfolios.map(p => benchmarkReturn),
                        backgroundColor: '#f59e0b',
                        borderRadius: 4
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
                    x: { ticks: { color: '#94a3b8' }, grid: { display: false } }
                },
                plugins: {
                    legend: { position: 'bottom', labels: { color: '#f8fafc' } }
                }
            }
        });
    }
}
