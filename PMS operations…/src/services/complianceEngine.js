export class ComplianceEngine {
    constructor() {}

    evaluateCompliance(complianceRecords) {
        const alerts = [];
        const now = new Date();

        const updatedRecords = complianceRecords.map(record => {
            let status = 'Compliant';
            const expiry = new Date(record.KYC_Expiry_Date);
            const diffDays = (expiry - now) / (1000 * 60 * 60 * 24);

            if (diffDays > 0 && diffDays <= 30) {
                status = 'Action Required';
                alerts.push({
                    type: 'KYC_EXPIRY',
                    message: `KYC for Client ${record.Client_ID} expires in ${Math.ceil(diffDays)} days.`
                });
            }

            if (record.FATCA_Status === 'Pending') {
                status = 'Action Required';
                alerts.push({
                    type: 'FATCA_MISSING',
                    message: `FATCA declaration missing for Client ${record.Client_ID}.`
                });
            }

            return {
                ...record,
                Compliance_Status: status
            };
        });

        return { alerts, updatedRecords };
    }
}
