"""
Diagnostic script — checks the BigQuery dates table schema and
the actual date ranges in the beneficiaries data.

Run:  python check_dates.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load .env the same way the app does
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text, create_engine

DATABASE_URL = os.getenv("DATABASE_URL", "").strip().strip('"').strip("'")
if not DATABASE_URL:
    print("❌ DATABASE_URL not set in .env"); sys.exit(1)

engine = create_engine(DATABASE_URL, echo=False)

def q(sql):
    with engine.connect() as conn:
        r = conn.execute(text(sql))
        cols = list(r.keys())
        rows = [dict(zip(cols, row)) for row in r.fetchall()]
        return cols, rows

print("=" * 60)
print("  BigQuery Diagnostic — dates table & registration ranges")
print("=" * 60)

# 1. Peek at the dates table — shows actual column names
print("\n[1] dates table — first 3 rows (shows real column names):")
try:
    cols, rows = q("SELECT * FROM `edw-pilot.neural.dates` LIMIT 3")
    print("    Columns:", cols)
    for r in rows:
        print("   ", r)
except Exception as e:
    print("    ERROR:", e)

# 2. registration_date_id range in beneficiaries
print("\n[2] registration_date_id range in beneficiaries:")
try:
    _, rows = q("""
        SELECT
          MIN(registration_date_id) AS min_id,
          MAX(registration_date_id) AS max_id,
          COUNT(*) AS total
        FROM `edw-pilot.neural.beneficiaries`
    """)
    print("   ", rows[0])
except Exception as e:
    print("    ERROR:", e)

# 3. payments date_id range
print("\n[3] date_id range in payments:")
try:
    _, rows = q("""
        SELECT
          MIN(date_id) AS min_id,
          MAX(date_id) AS max_id,
          COUNT(*) AS total
        FROM `edw-pilot.neural.payments`
    """)
    print("   ", rows[0])
except Exception as e:
    print("    ERROR:", e)

# 4. Try joining dates table with full_date (may fail — tells us the real column name)
print("\n[4] Testing JOIN with full_date column (if this errors, column is named differently):")
try:
    _, rows = q("""
        SELECT rd.full_date, COUNT(*) AS cnt
        FROM `edw-pilot.neural.beneficiaries` b
        JOIN `edw-pilot.neural.dates` rd ON b.registration_date_id = rd.date_id
        GROUP BY rd.full_date
        ORDER BY rd.full_date DESC
        LIMIT 5
    """)
    print("    OK — full_date column exists. Recent registration dates:")
    for r in rows:
        print("   ", r)
except Exception as e:
    print("    FAILED:", e)
    print("    → The column is NOT called 'full_date'. Fix needed in analytics.py.")

print("\n" + "=" * 60)
print("  Done. Share the output above to diagnose the filter issue.")
print("=" * 60)
