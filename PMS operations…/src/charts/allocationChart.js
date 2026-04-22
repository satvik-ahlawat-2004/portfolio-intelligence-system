export class AllocationChart {
    constructor(canvasId) {
        this.ctx = document.getElementById(canvasId);
        this.chart = null;
    }

    render(holdings) {
        const sectorMap = {};
        holdings.forEach(h => {
            sectorMap[h.Sector] = (sectorMap[h.Sector] || 0) + h.Holding_Value;
        });

        if (this.chart) this.chart.destroy();

        this.chart = new Chart(this.ctx, {
            type: 'doughnut',
            data: {
                labels: Object.keys(sectorMap),
                datasets: [{
                    data: Object.values(sectorMap),
                    backgroundColor: ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'bottom', labels: { color: '#f8fafc' } }
                }
            }
        });
    }
}
