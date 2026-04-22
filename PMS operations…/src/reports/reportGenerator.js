export class ReportGenerator {
    generateMonthlyReport(portfolios, benchmarkReturn) {
        return portfolios.map((p, idx) => {
            const outperformance = p.Return_Percentage - benchmarkReturn;
            
            // Automated commentary
            let outPerfText = outperformance > 0 ? `outperforming` : `underperforming`;
            let outPerfVal = Math.abs(outperformance).toFixed(2);
            
            let contributors = p.Holding_Contributions.slice(0, 2).map(c => c.Stock_Name.split('.')[0]).join(' and ');
            if(!contributors) contributors = 'diversified assets';
            
            const commentary = `The client portfolio generated ${p.Return_Percentage.toFixed(2)}% return over the period, ${outPerfText} the benchmark by ${outPerfVal}%. Major contributors were ${contributors}.`;

            return {
                Report_ID: 'REP' + (idx + 1).toString().padStart(3, '0'),
                Client_ID: p.Client_ID,
                Client_Name: p.Client_Name,
                Report_Type: 'Monthly Performance',
                Report_Date: new Date().toISOString().split('T')[0],
                Portfolio_Value: p.Portfolio_Value,
                Initial_Investment: p.Investment_Value,
                Profit_Loss: p.Profit_Loss,
                Return_Percentage: p.Return_Percentage,
                Benchmark_Return: benchmarkReturn,
                Outperformance: outperformance,
                Commentary: commentary
            };
        });
    }
}
