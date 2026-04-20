import requests
from datetime import datetime
import hashlib
from db import get_connection
import json

# 🔹 API config
url = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 2,
    "page": 1
}

# 🔹 Connect to DB
conn = get_connection()
cursor = conn.cursor()

# 🔹 Step 1: Check last run date (incremental control)
cursor.execute(
    "SELECT last_run_date FROM stg.pipeline_state WHERE pipeline_name = 'crypto_pipeline'"
)
result = cursor.fetchone()
last_run_date = result[0] if result else None

# 🔹 Current time
now = datetime.now()
today = now.date()

# 🔹 Step 2: Skip if already ran today
if last_run_date == today:
    print("Data for today has already been fetched. Exiting.")
    cursor.close()
    conn.close()
    exit()

# 🔹 Step 3: Call API (only if needed)
response = requests.get(url, params=params)
data = response.json()

# 🔹 Step 4: Insert query (idempotent)
query = """
INSERT INTO raw.crypto_market_raw
(coin_id, symbol, fetched_at, page_number, raw_payload, record_hash)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (record_hash) DO NOTHING
"""

# 🔹 Step 5: Process and insert data
for coin in data:
    row = {
        "coin_id": coin["id"],
        "symbol": coin["symbol"],
        "fetched_at": now,
        "page_number": 1,
        "raw_payload": json.dumps(coin),
        "record_hash": hashlib.md5(
            (coin["id"] + str(today)).encode()
        ).hexdigest()
    }

    values = (
        row["coin_id"],
        row["symbol"],
        row["fetched_at"],
        row["page_number"],
        row["raw_payload"],
        row["record_hash"]
    )

    cursor.execute(query, values)

# 🔹 Step 6: Update pipeline state
cursor.execute(
    "UPDATE stg.pipeline_state SET last_run_date = %s WHERE pipeline_name = 'crypto_pipeline'",
    (today,)
)

# 🔹 Step 7: Commit all changes
conn.commit()

# 🔹 Step 8: Close connection
cursor.close()
conn.close()

print("Data inserted successfully!")