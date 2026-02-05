from datetime import date
from fastapi import APIRouter, Query, Depends
from sqlalchemy import select, func
from app.db import SessionLocal
from app.models import DmSalesDaily, DmSalesByProduct
from app.security import get_current_user
router = APIRouter(prefix="/sales", tags=["sales"])

@router.get("/daily")
async def sales_daily(
    user=Depends(get_current_user), 
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    async with SessionLocal() as session:
        stmt = select(DmSalesDaily)
        count_stmt = select(func.count()).select_from(DmSalesDaily)

        if start_date:
            stmt = stmt.where(DmSalesDaily.date >= start_date)
            count_stmt = count_stmt.where(DmSalesDaily.date >= start_date)
        if end_date:
            stmt = stmt.where(DmSalesDaily.date <= end_date)
            count_stmt = count_stmt.where(DmSalesDaily.date <= end_date)

        total = (await session.execute(count_stmt)).scalar_one()
        rows = (
            await session.execute(stmt.order_by(DmSalesDaily.date).limit(limit).offset(offset))
        ).scalars().all()

        items = [
            {
                "date": r.date,
                "total_revenue": float(r.total_revenue),
                "total_orders": r.total_orders,
                "avg_order_value": float(r.avg_order_value),
            }
            for r in rows
        ]
        return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/by-product")
async def sales_by_product(
    category: str | None = None,
    brand: str | None = None,
    min_revenue: float | None = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    async with SessionLocal() as session:
        stmt = select(DmSalesByProduct)
        count_stmt = select(func.count()).select_from(DmSalesByProduct)

        if category:
            stmt = stmt.where(DmSalesByProduct.category == category)
            count_stmt = count_stmt.where(DmSalesByProduct.category == category)
        if brand:
            stmt = stmt.where(DmSalesByProduct.brand == brand)
            count_stmt = count_stmt.where(DmSalesByProduct.brand == brand)
        if min_revenue is not None:
            stmt = stmt.where(DmSalesByProduct.total_revenue >= min_revenue)
            count_stmt = count_stmt.where(DmSalesByProduct.total_revenue >= min_revenue)

        total = (await session.execute(count_stmt)).scalar_one()
        rows = (
            await session.execute(
                stmt.order_by(DmSalesByProduct.total_revenue.desc()).limit(limit).offset(offset)
            )
        ).scalars().all()

        items = [
            {
                "product_id": r.product_id,
                "product_name": r.product_name,
                "category": r.category,
                "brand": r.brand,
                "total_quantity_sold": r.total_quantity_sold,
                "total_revenue": float(r.total_revenue),
            }
            for r in rows
        ]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
