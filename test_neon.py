import psycopg2

NEON_URL = "postgresql://neondb_owner:npg_fCQNF8Hh5azw@ep-delicate-grass-a1wh92wv-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

try:
    conn = psycopg2.connect(NEON_URL)
    cur = conn.cursor()
    cur.execute("SELECT version();")
    print("✅ Connected:", cur.fetchone())
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    conn.commit()
    print("✅ pgvector extension ready")
    conn.close()
except Exception as e:
    print("❌ Error:", e)