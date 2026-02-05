from fastapi import APIRouter, Query , Depends
from sqlalchemy import select, func
from app.db import SessionLocal
from app.models import DmProductReviewsImpact
from app.security import get_current_user
router = APIRouter(prefix="/reviews", tags=["reviews"])

@router.get("/impact")
async def reviews_impact(
    user=Depends(get_current_user),
    min_rating: float | None = None,
    min_reviews: int | None = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    async with SessionLocal() as session:
        stmt = select(DmProductReviewsImpact)
        count_stmt = select(func.count()).select_from(DmProductReviewsImpact)

        if min_rating is not None:
            stmt = stmt.where(DmProductReviewsImpact.avg_rating >= min_rating)
            count_stmt = count_stmt.where(DmProductReviewsImpact.avg_rating >= min_rating)
        if min_reviews is not None:
            stmt = stmt.where(DmProductReviewsImpact.nb_reviews >= min_reviews)
            count_stmt = count_stmt.where(DmProductReviewsImpact.nb_reviews >= min_reviews)

        total = (await session.execute(count_stmt)).scalar_one()
        rows = (
            await session.execute(
                stmt.order_by(DmProductReviewsImpact.total_revenue.desc()).limit(limit).offset(offset)
            )
        ).scalars().all()

        items = [
            {
                "product_id": r.product_id,
                "product_name": r.product_name,
                "avg_rating": float(r.avg_rating) if r.avg_rating is not None else None,
                "nb_reviews": r.nb_reviews,
                "total_revenue": float(r.total_revenue),
            }
            for r in rows
        ]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
