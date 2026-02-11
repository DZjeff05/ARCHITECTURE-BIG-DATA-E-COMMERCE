import os
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError

from db import get_conn
from auth import authenticate, create_access_token, decode_token

app = FastAPI(
    title="E-commerce Gold API",
    version="1.0.0",
    description="API REST sécurisée JWT exposant les tables de la couche GOLD (1 table = 1 endpoint)."
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

PG_SCHEMA = os.getenv("PG_SCHEMA", "datamart")

# Table whitelist (sécurité + évite injection)
TABLES = {
    "dm_sales_daily": {
        "endpoint": "/gold/sales/daily",
        "default_sort": "date"
    },
    "dm_sales_by_product": {
        "endpoint": "/gold/sales/by-product",
        "default_sort": "total_revenue"
    },
    "dm_customer_metrics": {
        "endpoint": "/gold/customers/metrics",
        "default_sort": "total_spent"
    },
    "dm_product_reviews_impact": {
        "endpoint": "/gold/products/reviews-impact",
        "default_sort": "total_revenue"
    },
    "dm_stock_performance": {
        "endpoint": "/gold/stock/performance",
        "default_sort": "stock_turnover_ratio"
    },
}


def require_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = decode_token(token)
        return payload.get("sub")
    except JWTError:
        raise HTTPException(status_code=401, detail="Token invalide ou expiré")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/auth/login")
def login(form: OAuth2PasswordRequestForm = Depends()):
    if not authenticate(form.username, form.password):
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    token = create_access_token(sub=form.username)
    return {"access_token": token, "token_type": "bearer"}


@app.get("/gold/tables", dependencies=[Depends(require_user)])
def list_gold_tables():
    """Liste les tables GOLD exposées + leurs endpoints."""
    items = []
    for t, cfg in TABLES.items():
        items.append({"table": t, "endpoint": cfg["endpoint"]})
    return {"tables": items}


def _get_columns(schema, table):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position
            """, (schema, table))
            return [r["column_name"] for r in cur.fetchall()]


def _fetch_table(table_name, page, page_size, order_by, order_dir):
    if table_name not in TABLES:
        raise HTTPException(status_code=404, detail="Table GOLD inconnue")

    cols = _get_columns(PG_SCHEMA, table_name)
    sort_col = order_by or TABLES[table_name]["default_sort"]

    if sort_col not in cols:
        raise HTTPException(status_code=400, detail="order_by invalide")

    offset = (page - 1) * page_size

    with get_conn() as conn:
        with conn.cursor() as cur:
            # count total
            cur.execute('SELECT COUNT(*) AS cnt FROM "{}"."{}"'.format(PG_SCHEMA, table_name))
            total = int(cur.fetchone()["cnt"])

            # fetch data
            q = 'SELECT * FROM "{}"."{}" ORDER BY "{}" {} LIMIT %s OFFSET %s'.format(
                PG_SCHEMA, table_name, sort_col, order_dir.upper()
            )
            cur.execute(q, (page_size, offset))
            rows = cur.fetchall()

    return {
        "table": table_name,
        "schema": PG_SCHEMA,
        "page": page,
        "page_size": page_size,
        "total_rows": total,
        "total_pages": (total + page_size - 1) // page_size,
        "order_by": sort_col,
        "order_dir": order_dir,
        "columns": cols,
        "data": rows
    }


# ==========
# ENDPOINTS (1 table = 1 endpoint)
# ==========

@app.get("/gold/sales/daily", dependencies=[Depends(require_user)])
def gold_sales_daily(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    order_by: str = Query(None),
    order_dir: str = Query("desc", pattern="^(asc|desc)$")
):
    return _fetch_table("dm_sales_daily", page, page_size, order_by, order_dir)


@app.get("/gold/sales/by-product", dependencies=[Depends(require_user)])
def gold_sales_by_product(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    order_by: str = Query(None),
    order_dir: str = Query("desc", pattern="^(asc|desc)$")
):
    return _fetch_table("dm_sales_by_product", page, page_size, order_by, order_dir)


@app.get("/gold/customers/metrics", dependencies=[Depends(require_user)])
def gold_customer_metrics(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    order_by: str = Query(None),
    order_dir: str = Query("desc", pattern="^(asc|desc)$")
):
    return _fetch_table("dm_customer_metrics", page, page_size, order_by, order_dir)


@app.get("/gold/products/reviews-impact", dependencies=[Depends(require_user)])
def gold_reviews_impact(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    order_by: str = Query(None),
    order_dir: str = Query("desc", pattern="^(asc|desc)$")
):
    return _fetch_table("dm_product_reviews_impact", page, page_size, order_by, order_dir)


@app.get("/gold/stock/performance", dependencies=[Depends(require_user)])
def gold_stock_performance(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    order_by: str = Query(None),
    order_dir: str = Query("desc", pattern="^(asc|desc)$")
):
    return _fetch_table("dm_stock_performance", page, page_size, order_by, order_dir)
