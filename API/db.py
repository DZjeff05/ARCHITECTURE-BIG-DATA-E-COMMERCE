import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres-gold"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "gold"),
        user=os.getenv("PG_USER", "gold_user"),
        password=os.getenv("PG_PASSWORD", "gold_pwd"),
        cursor_factory=RealDictCursor
    )
