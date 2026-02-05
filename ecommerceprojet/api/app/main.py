from fastapi import FastAPI
from sqlalchemy import text
from app.db import engine

from app.routers.auth import router as auth_router
from app.routers.sales import router as sales_router
from app.routers.customers import router as customers_router
from app.routers.reviews import router as reviews_router
from app.routers.stocks import router as stocks_router

app = FastAPI(title="E-commerce Gold API")

app.include_router(auth_router)
app.include_router(sales_router)
app.include_router(customers_router)
app.include_router(reviews_router)
app.include_router(stocks_router)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/db-check")
async def db_check():
    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT 1 as ok;"))
        row = res.first()
    return {"db": "ok", "value": row.ok}
