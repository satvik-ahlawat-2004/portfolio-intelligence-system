// Mock Google Sheets Database
const DB = {
    Clients: [
        { Client_ID: 'C001', Client_Name: 'Rahul Sharma', PAN: 'ABCDE1234F', Risk_Profile: 'Aggressive', Investment_Amount: 5000000, Investment_Date: '2023-01-15', Email: 'rahul@example.com', Phone: '9876543210', Status: 'Active' },
        { Client_ID: 'C002', Client_Name: 'Priya Patel', PAN: 'FGHIJ5678K', Risk_Profile: 'Moderate', Investment_Amount: 3000000, Investment_Date: '2023-03-10', Email: 'priya@example.com', Phone: '9876543211', Status: 'Active' },
        { Client_ID: 'C003', Client_Name: 'Amit Singh', PAN: 'KLMNO9012P', Risk_Profile: 'Conservative', Investment_Amount: 10000000, Investment_Date: '2022-11-05', Email: 'amit@example.com', Phone: '9876543212', Status: 'Inactive' }
    ],
    Holdings: [
        { Holding_ID: 'H001', Client_ID: 'C001', Stock_Name: 'Reliance Ind.', Sector: 'Energy', Quantity: 500, Buy_Price: 2400, Current_Price: 2900, Investment_Value: 1200000, Current_Value: 1450000 },
        { Holding_ID: 'H002', Client_ID: 'C001', Stock_Name: 'TCS', Sector: 'IT', Quantity: 300, Buy_Price: 3200, Current_Price: 3900, Investment_Value: 960000, Current_Value: 1170000 },
        { Holding_ID: 'H003', Client_ID: 'C002', Stock_Name: 'HDFC Bank', Sector: 'Banking', Quantity: 1000, Buy_Price: 1500, Current_Price: 1650, Investment_Value: 1500000, Current_Value: 1650000 },
        { Holding_ID: 'H004', Client_ID: 'C003', Stock_Name: 'Infosys', Sector: 'IT', Quantity: 2000, Buy_Price: 1400, Current_Price: 1700, Investment_Value: 2800000, Current_Value: 3400000 }
    ],
    Transactions: [
        { Transaction_ID: 'T001', Client_ID: 'C001', Stock_Name: 'Reliance Ind.', Transaction_Type: 'BUY', Quantity: 500, Price: 2400, Transaction_Date: '2023-05-12' },
        { Transaction_ID: 'T002', Client_ID: 'C002', Stock_Name: 'HDFC Bank', Transaction_Type: 'BUY', Quantity: 1000, Price: 1500, Transaction_Date: '2023-06-01' }
    ],
    AUM_Tracker: [
        { Date: '2023-10-01', Total_AUM: 18000000, New_Investments: 0, Withdrawals: 0, Market_Gain_Loss: 200000, Fees_Deducted: 10000 }
    ],
    Performance: [],
    Compliance: [
        { Client_ID: 'C001', KYC_Status: 'Verified', PAN_Verified: 'Yes', FATCA_Status: 'Done', Risk_Profile: 'Aggressive', KYC_Expiry_Date: '2024-05-01', Compliance_Status: 'Compliant' },
        { Client_ID: 'C002', KYC_Status: 'Pending', PAN_Verified: 'Yes', FATCA_Status: 'Pending', Risk_Profile: 'Moderate', KYC_Expiry_Date: '2024-04-12', Compliance_Status: 'Action Required' },
        { Client_ID: 'C003', KYC_Status: 'Verified', PAN_Verified: 'Yes', FATCA_Status: 'Done', Risk_Profile: 'Conservative', KYC_Expiry_Date: '2027-11-05', Compliance_Status: 'Compliant' }
    ],
    Reports: []
};

// Global config
const CONFIG = {
    Benchmark_Return: 12.5 // Example NIFTY 50 return
};
