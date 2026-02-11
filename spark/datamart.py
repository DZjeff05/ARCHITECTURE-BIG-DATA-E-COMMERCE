# -*- coding: utf-8 -*-

import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, countDistinct, sum as Fsum, avg as Favg,
    min as Fmin, max as Fmax, when, lit
)

# ====================================================
# CONFIG
# ====================================================
HIVE_DB = "ecommerce_silver"

POSTGRES_URL = "jdbc:postgresql://postgres-gold:5432/gold"
POSTGRES_USER = "gold_user"
POSTGRES_PASSWORD = "gold_pwd"
POSTGRES_SCHEMA = "datamart"

# ====================================================
# LOGGING
# ====================================================
LOG_DIR = "/project/logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "gold.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def write_pg(df, table_name, mode="overwrite"):
    full_table = "{}.{}".format(POSTGRES_SCHEMA, table_name)
    logging.info("Writing PostgreSQL table: {}".format(full_table))

    (df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", full_table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )


def main():
    logging.info("===== START GOLD JOB =====")

    spark = (SparkSession.builder
             .appName("Gold_Datamart_Ecommerce")
             .config("hive.metastore.uris", "thrift://hive-metastore:9083")
             .enableHiveSupport()
             .getOrCreate())

    # -------------------------
    # READ FROM HIVE (SILVER)
    # -------------------------
    spark.sql("USE {}".format(HIVE_DB))

    fact_orders = spark.table("fact_orders")
    fact_order_items = spark.table("fact_order_items")
    dim_products = spark.table("dim_products")
    dim_customers = spark.table("dim_customers")
    fact_reviews = spark.table("fact_reviews")

    # ====================================================
    # 1) dm_sales_daily(date, total_revenue, total_orders, avg_order_value)
    # ====================================================
    orders_day = fact_orders.select(
        to_date(col("order_date")).alias("date"),
        col("order_id"),
        col("total_amount")
    )

    dm_sales_daily = (orders_day
        .groupBy("date")
        .agg(
            Fsum("total_amount").alias("total_revenue"),
            countDistinct("order_id").alias("total_orders")
        )
        .withColumn(
            "avg_order_value",
            when(col("total_orders") > 0, col("total_revenue") / col("total_orders")).otherwise(lit(0.0))
        )
    )

    # ====================================================
    # 2) dm_sales_by_product(product_id, product_name, category, brand, total_quantity_sold, total_revenue)
    # ====================================================
    items_enriched = (fact_order_items
        .join(dim_products.select("product_id", "product_name", "category", "brand"), "product_id", "left")
        .select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("brand"),
            col("quantity"),
            col("line_amount")
        )
    )

    dm_sales_by_product = (items_enriched
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            Fsum("quantity").alias("total_quantity_sold"),
            Fsum("line_amount").alias("total_revenue")
        )
    )

    # ====================================================
    # 3) dm_customer_metrics(customer_id, country, total_orders, total_spent, avg_order_value, first_order_date, last_order_date)
    # ====================================================
    orders_customer = (fact_orders
        .join(dim_customers.select("customer_id", "country"), "customer_id", "left")
        .select(
            col("customer_id"),
            col("country"),
            to_date(col("order_date")).alias("order_date"),
            col("order_id"),
            col("total_amount")
        )
    )

    dm_customer_metrics = (orders_customer
        .groupBy("customer_id", "country")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            Fsum("total_amount").alias("total_spent"),
            Fmin("order_date").alias("first_order_date"),
            Fmax("order_date").alias("last_order_date")
        )
        .withColumn(
            "avg_order_value",
            when(col("total_orders") > 0, col("total_spent") / col("total_orders")).otherwise(lit(0.0))
        )
    )

    # ====================================================
    # 4) dm_product_reviews_impact(product_id, product_name, avg_rating, nb_reviews, total_revenue)
    # ====================================================
    reviews_agg = (fact_reviews
        .groupBy("product_id")
        .agg(
            Favg("rating").alias("avg_rating"),
            countDistinct("review_id").alias("nb_reviews")
        )
    )

    sales_revenue_by_product = (fact_order_items
        .groupBy("product_id")
        .agg(
            Fsum("line_amount").alias("total_revenue")
        )
    )

    dm_product_reviews_impact = (dim_products
        .select("product_id", "product_name")
        .join(reviews_agg, "product_id", "left")
        .join(sales_revenue_by_product, "product_id", "left")
        .na.fill({"avg_rating": 0.0, "nb_reviews": 0, "total_revenue": 0.0})
        .select("product_id", "product_name", "avg_rating", "nb_reviews", "total_revenue")
    )

    # ====================================================
    # 5) dm_stock_performance(product_id, product_name, category, stock_quantity, total_quantity_sold, stock_turnover_ratio)
    #    stock_turnover_ratio = total_quantity_sold / stock_quantity  (simple & actionnable)
    # ====================================================
    sold_qty = (fact_order_items
        .groupBy("product_id")
        .agg(Fsum("quantity").alias("total_quantity_sold"))
    )

    dm_stock_performance = (dim_products
        .select("product_id", "product_name", "category", "stock_quantity")
        .join(sold_qty, "product_id", "left")
        .na.fill({"total_quantity_sold": 0})
        .withColumn(
            "stock_turnover_ratio",
            when(col("stock_quantity") > 0, col("total_quantity_sold") / col("stock_quantity")).otherwise(lit(0.0))
        )
        .select("product_id", "product_name", "category", "stock_quantity", "total_quantity_sold", "stock_turnover_ratio")
    )

    # ====================================================
    # WRITE TO POSTGRES (GOLD)
    # ====================================================
    write_pg(dm_sales_daily, "dm_sales_daily", mode="overwrite")
    write_pg(dm_sales_by_product, "dm_sales_by_product", mode="overwrite")
    write_pg(dm_customer_metrics, "dm_customer_metrics", mode="overwrite")
    write_pg(dm_product_reviews_impact, "dm_product_reviews_impact", mode="overwrite")
    write_pg(dm_stock_performance, "dm_stock_performance", mode="overwrite")

    logging.info("===== GOLD JOB SUCCESS =====")
    spark.stop()


if __name__ == "__main__":
    main()
