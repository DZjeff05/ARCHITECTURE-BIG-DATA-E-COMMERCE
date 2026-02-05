from fastapi import APIRouter, Query,Depends
from sqlalchemy import select, func
from app.db import SessionLocal
from app.models import DmCustomerMetrics
from app.security import get_current_user
router = APIRouter(prefix="/customers", tags=["customers"])

@router.get("/metrics")
async def customers_metrics(
    user=Depends(get_current_user),
    country: str | None = None,
    min_orders: int | None = None,
    min_spent: float | None = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    async with SessionLocal() as session:
        stmt = select(DmCustomerMetrics)
        count_stmt = select(func.count()).select_from(DmCustomerMetrics)

        if country:
            stmt = stmt.where(DmCustomerMetrics.country == country)
            count_stmt = count_stmt.where(DmCustomerMetrics.country == country)
        if min_orders is not None:
            stmt = stmt.where(DmCustomerMetrics.total_orders >= min_orders)
            count_stmt = count_stmt.where(DmCustomerMetrics.total_orders >= min_orders)
        if min_spent is not None:
            stmt = stmt.where(DmCustomerMetrics.total_spent >= min_spent)
            count_stmt = count_stmt.where(DmCustomerMetrics.total_spent >= min_spent)

        total = (await session.execute(count_stmt)).scalar_one()
        rows = (
            await session.execute(
                stmt.order_by(DmCustomerMetrics.total_spent.desc()).limit(limit).offset(offset)
            )
        ).scalars().all()

        items = [
            {
                "customer_id": r.customer_id,
                "country": r.country,
                "total_orders": r.total_orders,
                "total_spent": float(r.total_spent),
                "avg_order_value": float(r.avg_order_value),
                "first_order_date": r.first_order_date,
                "last_order_date": r.last_order_date,
            }
            for r in rows
        ]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
