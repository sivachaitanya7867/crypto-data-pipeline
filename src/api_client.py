import requests
from datetime import datetime
import hashlib
from db import get_connection
import json

url = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 2,
    "page": 1
}

response = requests.get(url, params=params)
data = response.json()

conn = get_connection()
cursor = conn.cursor()

query = """INSERT INTO raw.crypto_market_raw (coin_id, symbol, fetched_at, page_number, raw_payload, record_hash) 
        VALUES (%s, %s, %s, %s, %s, %s)"""

for coin in data:
    row = {
    "coin_id": coin["id"],
    "symbol": coin["symbol"],
    "fetched_at": datetime.now(),
    "page_number": 1,
    "raw_payload": json.dumps(coin),
    "record_hash": hashlib.md5(str(coin).encode()).hexdigest()
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

conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully!")