import os
import psycopg2
from web3 import Web3
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
QUICKNODE_URL = os.getenv("QUICKNODE_URL")
DB_URI = os.getenv("DB_URI")

# Connect to QuickNode
web3 = Web3(Web3.HTTPProvider(QUICKNODE_URL))
assert web3.is_connected(), "Failed to connect to QuickNode"

# Connect to database
conn = psycopg2.connect(DB_URI)
cur = conn.cursor()

# Create transactions table
cur.execute("""
CREATE TABLE IF NOT EXISTS arbitrum_transactions (
    hash TEXT PRIMARY KEY,
    from_address TEXT,
    to_address TEXT,
    block_number BIGINT,
    value NUMERIC,
    gas BIGINT,
    timestamp TIMESTAMP
);
""")

# Track last processed block
cur.execute("""
CREATE TABLE IF NOT EXISTS block_tracker (
    id SERIAL PRIMARY KEY,
    last_block BIGINT
);
""")
conn.commit()

# Get last processed block or start from latest - N
cur.execute("SELECT last_block FROM block_tracker ORDER BY id DESC LIMIT 1;")
row = cur.fetchone()
start_block = row[0] + 1 if row else web3.eth.block_number - 5  # backfill last 5 blocks if first time
end_block = web3.eth.block_number

print(f"📦 Processing blocks from {start_block} to {end_block}")

# Loop over blocks
for block_num in range(start_block, end_block + 1):
    block = web3.eth.get_block(block_num, full_transactions=True)
    timestamp = datetime.fromtimestamp(block.timestamp)

    for tx in block.transactions:
        tx_hash = tx.hash.hex()
        from_address = tx["from"]
        to_address = tx["to"] if tx["to"] else None
        value = web3.from_wei(tx.value, 'ether')
        gas = tx.gas

        cur.execute("""
            INSERT INTO arbitrum_transactions (
                hash, from_address, to_address, block_number, value, gas, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hash) DO NOTHING;
        """, (tx_hash, from_address, to_address, block_num, value, gas, timestamp))

    # Update tracker table
    cur.execute("INSERT INTO block_tracker (last_block) VALUES (%s);", (block_num,))
    conn.commit()
    print(f"✅ Block {block_num} with {len(block.transactions)} transactions inserted")

# Clean up
cur.close()
conn.close()
print("🎉 ETL pipeline completed successfully!")
