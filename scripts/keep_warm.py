"""
Neon Keep-Warm Script
Pings /health every 4 minutes to prevent Neon free tier from auto-suspending.
Run in a separate terminal: python scripts/keep_warm.py
"""
import time, urllib.request, json, datetime

URL = "http://localhost:8000/health"
INTERVAL = 240  # 4 minutes

print(f"Keep-warm started — pinging {URL} every {INTERVAL}s")
print("Press Ctrl+C to stop\n")

while True:
    try:
        with urllib.request.urlopen(URL, timeout=15) as r:
            data = json.loads(r.read())
            neon = data.get("neon_pgvector", "?")
            bq   = data.get("bigquery", "?")
            ts   = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] OK — BigQuery: {bq} | Neon: {neon}")
    except Exception as e:
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] WARN — {e}")
    time.sleep(INTERVAL)
