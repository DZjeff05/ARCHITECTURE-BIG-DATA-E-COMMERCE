from fastapi import APIRouter, Query,Depends
from sqlalchemy import select, func
from app.db import SessionLocal
from app.models import DmStockPerformance
from app.security import get_current_user
router = APIRouter(prefix="/stocks", tags=["stocks"])

@router.get("/performance")
async def stock_performance(
    user=Depends(get_current_user),
    category: str | None = None,
    min_stock: int | None = None,
    min_turnover: float | None = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    async with SessionLocal() as session:
        stmt = select(DmStockPerformance)
        count_stmt = select(func.count()).select_from(DmStockPerformance)

        if category:
            stmt = stmt.where(DmStockPerformance.category == category)
            count_stmt = count_stmt.where(DmStockPerformance.category == category)
        if min_stock is not None:
            stmt = stmt.where(DmStockPerformance.stock_quantity >= min_stock)
            count_stmt = count_stmt.where(DmStockPerformance.stock_quantity >= min_stock)
        if min_turnover is not None:
            stmt = stmt.where(DmStockPerformance.stock_turnover_ratio >= min_turnover)
            count_stmt = count_stmt.where(DmStockPerformance.stock_turnover_ratio >= min_turnover)

        total = (await session.execute(count_stmt)).scalar_one()
        rows = (
            await session.execute(
                stmt.order_by(DmStockPerformance.stock_turnover_ratio.desc().nullslast())
                .limit(limit)
                .offset(offset)
            )
        ).scalars().all()

        items = [
            {
                "product_id": r.product_id,
                "product_name": r.product_name,
                "category": r.category,
                "stock_quantity": r.stock_quantity,
                "total_quantity_sold": r.total_quantity_sold,
                "stock_turnover_ratio": float(r.stock_turnover_ratio) if r.stock_turnover_ratio is not None else None,
            }
            for r in rows
        ]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
