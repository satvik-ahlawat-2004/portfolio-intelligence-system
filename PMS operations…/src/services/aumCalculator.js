export class AumCalculator {
    calculateTotalAum(portfolios) {
        const totalAum = portfolios.reduce((sum, p) => sum + p.Portfolio_Value, 0);
        return totalAum;
    }

    createAumRecord(totalAum) {
        return {
            Date: new Date().toISOString().split('T')[0],
            Total_AUM: totalAum,
            New_Investments: 0,
            Withdrawals: 0,
            Market_Gain_Loss: 0,
            Fees_Deducted: 0
        };
    }
}
