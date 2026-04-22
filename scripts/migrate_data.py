import pandas as pd
import os
from scripts.storage_manager import StorageManager

# Paths
DATA_DIR = "data"
OLD_CLIENTS_PATH = os.path.join(DATA_DIR, "clients.csv.old") # I'll rename the current one

def migrate():
    # Rename current clients.csv to keep a backup
    if os.path.exists(os.path.join(DATA_DIR, "clients.csv")) and not os.path.exists(OLD_CLIENTS_PATH):
        os.rename(os.path.join(DATA_DIR, "clients.csv"), OLD_CLIENTS_PATH)
        print("Backed up old clients.csv to clients.csv.old")
    
    if not os.path.exists(OLD_CLIENTS_PATH):
        print("No old clients data found for migration.")
        return

    old_df = pd.read_csv(OLD_CLIENTS_PATH)
    sm = StorageManager()
    
    # Check if we already migrated
    existing_clients = sm.get_clients()
    if not existing_clients.empty:
        print("Migration already seems to have occurred. Skipping.")
        return

    for _, row in old_df.iterrows():
        cid = row['ClientID']
        cname = row['ClientName']
        asset = row['Asset']
        amount = row['InvestmentAmount']
        date = row['EntryDate']
        
        # Add client
        sm.add_client(cid, cname, "Medium") # Default risk
        # Add transaction
        # For simple migration, we assume price is 1 since we only have amount. 
        # But wait, the engine prefers quantity and price. 
        # I'll just use price as amount and quantity as 1 for legacy data if needed.
        sm.add_transaction(cid, asset, "BUY", 1, amount, date)
        
    print(f"Migrated {len(old_df)} clients.")

if __name__ == "__main__":
    migrate()
