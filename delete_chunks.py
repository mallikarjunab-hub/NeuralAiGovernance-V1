import psycopg2

conn = psycopg2.connect(
    "postgresql://neondb_owner:npg_OzbU70gEiYuV@ep-billowing-tooth-a14x195c.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
)
cur = conn.cursor()
cur.execute("DELETE FROM document_chunks WHERE doc_name='DSSY_Knowledge_Base'")
conn.commit()
print("Deleted rows:", cur.rowcount)
conn.close()