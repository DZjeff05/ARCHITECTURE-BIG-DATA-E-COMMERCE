from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Date, Integer, Numeric, Text

class Base(DeclarativeBase):
    pass

class DmSalesDaily(Base):
    __tablename__ = "dm_sales_daily"
    __table_args__ = {"schema": "gold"}

    date: Mapped[str] = mapped_column(Date, primary_key=True)
    total_revenue: Mapped[float] = mapped_column(Numeric(18, 2))
    total_orders: Mapped[int] = mapped_column(Integer)
    avg_order_value: Mapped[float] = mapped_column(Numeric(18, 2))
class DmSalesByProduct(Base):
    __tablename__ = "dm_sales_by_product"
    __table_args__ = {"schema": "gold"}

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product_name: Mapped[str] = mapped_column(Text)
    category: Mapped[str | None] = mapped_column(Text, nullable=True)
    brand: Mapped[str | None] = mapped_column(Text, nullable=True)
    total_quantity_sold: Mapped[int] = mapped_column(Integer)
    total_revenue: Mapped[float] = mapped_column(Numeric(18, 2))

class DmCustomerMetrics(Base):
    __tablename__ = "dm_customer_metrics"
    __table_args__ = {"schema": "gold"}

    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    country: Mapped[str | None] = mapped_column(Text, nullable=True)
    total_orders: Mapped[int] = mapped_column(Integer)
    total_spent: Mapped[float] = mapped_column(Numeric(18, 2))
    avg_order_value: Mapped[float] = mapped_column(Numeric(18, 2))
    first_order_date: Mapped[str | None] = mapped_column(Date, nullable=True)
    last_order_date: Mapped[str | None] = mapped_column(Date, nullable=True)

class DmProductReviewsImpact(Base):
    __tablename__ = "dm_product_reviews_impact"
    __table_args__ = {"schema": "gold"}

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product_name: Mapped[str] = mapped_column(Text)
    avg_rating: Mapped[float | None] = mapped_column(Numeric(3, 2), nullable=True)
    nb_reviews: Mapped[int] = mapped_column(Integer)
    total_revenue: Mapped[float] = mapped_column(Numeric(18, 2))

class DmStockPerformance(Base):
    __tablename__ = "dm_stock_performance"
    __table_args__ = {"schema": "gold"}

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product_name: Mapped[str] = mapped_column(Text)
    category: Mapped[str | None] = mapped_column(Text, nullable=True)
    stock_quantity: Mapped[int] = mapped_column(Integer)
    total_quantity_sold: Mapped[int] = mapped_column(Integer)
    stock_turnover_ratio: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)