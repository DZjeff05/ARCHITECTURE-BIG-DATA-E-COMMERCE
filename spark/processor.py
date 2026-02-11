# -*- coding: utf-8 -*-

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date,
    year, month, dayofmonth, weekofyear, quarter
)
from pyspark.sql.types import IntegerType, DoubleType

# ====================================================
# CONFIG
# ====================================================
BRONZE_PATH = "hdfs://namenode:9000/data/raw"
SILVER_PATH = "hdfs://namenode:9000/data/silver"

# ====================================================
# LOGGING
# ====================================================
LOG_DIR = "/project/logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "processor.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ====================================================
# UTILS
# ====================================================
def read_bronze(spark, table, y, m, d):
    path = "{}/{}/year={}/month={}/day={}".format(
        BRONZE_PATH, table, y, m, d
    )
    logging.info("Reading BRONZE path: {}".format(path))
    return spark.read.parquet(path)

def write_silver(df, table, y, m, d):
    output_path = "{}/{}".format(SILVER_PATH, table)

    df_out = df \
        .withColumn("ingest_year", lit(y)) \
        .withColumn("ingest_month", lit(m)) \
        .withColumn("ingest_day", lit(d))

    df_out.write \
        .mode("append") \
        .partitionBy("ingest_year", "ingest_month", "ingest_day") \
        .parquet(output_path)

    logging.info("Wrote SILVER table {} to {}".format(table, output_path))

# ====================================================
# MAIN
# ====================================================
def main():
    logging.info("===== START SILVER PROCESSOR =====")

    spark = SparkSession.builder \
        .appName("Silver_Processor_Ecommerce") \
        .getOrCreate()

    now = datetime.now()
    y, m, d = now.year, now.month, now.day

    try:
        # -------------------------
        # READ BRONZE
        # -------------------------
        customers_b = read_bronze(spark, "customers", y, m, d)
        products_b = read_bronze(spark, "products", y, m, d)
        orders_b = read_bronze(spark, "orders", y, m, d)
        order_items_b = read_bronze(spark, "order_items", y, m, d)
        reviews_b = read_bronze(spark, "product_reviews", y, m, d)

        # -------------------------
        # DIM CUSTOMERS
        # -------------------------
        dim_customers = customers_b.select(
            col("customer_id").cast(IntegerType()).alias("customer_id"),
            col("gender").alias("gender"),
            col("country").alias("country"),
            to_date(col("signup_date")).alias("signup_date")
        ).dropna(subset=["customer_id"]) \
         .dropDuplicates(["customer_id"])

        # -------------------------
        # DIM PRODUCTS
        # -------------------------
        dim_products = products_b.select(
            col("product_id").cast(IntegerType()).alias("product_id"),
            col("product_name").alias("product_name"),
            col("category").alias("category"),
            col("brand").alias("brand"),
            col("price").cast(DoubleType()).alias("price"),
            col("stock_quantity").cast(IntegerType()).alias("stock_quantity")
        ).dropna(subset=["product_id"]) \
         .dropDuplicates(["product_id"])

        # -------------------------
        # FACT ORDERS
        # -------------------------
        fact_orders = orders_b.select(
            col("order_id").cast(IntegerType()).alias("order_id"),
            col("customer_id").cast(IntegerType()).alias("customer_id"),
            to_date(col("order_date")).alias("order_date"),
            col("total_amount").cast(DoubleType()).alias("total_amount"),
            col("payment_method").alias("payment_method"),
            col("shipping_country").alias("shipping_country")
        ).filter(col("total_amount") > 0) \
         .dropDuplicates(["order_id"])

        # FK validation orders -> customers
        fact_orders = fact_orders.join(
            dim_customers.select("customer_id"),
            on="customer_id",
            how="inner"
        )

        # -------------------------
        # FACT ORDER ITEMS
        # -------------------------
        fact_order_items = order_items_b.select(
            col("order_item_id").cast(IntegerType()).alias("order_item_id"),
            col("order_id").cast(IntegerType()).alias("order_id"),
            col("product_id").cast(IntegerType()).alias("product_id"),
            col("quantity").cast(IntegerType()).alias("quantity"),
            col("unit_price").cast(DoubleType()).alias("unit_price")
        ).filter(
            (col("quantity") > 0) & (col("unit_price") > 0)
        ).dropDuplicates(["order_item_id"])

        # join order_items -> orders (get order_date)
        fact_order_items = fact_order_items.join(
            fact_orders.select("order_id", "order_date"),
            on="order_id",
            how="inner"
        )

        # FK validation order_items -> products
        fact_order_items = fact_order_items.join(
            dim_products.select("product_id"),
            on="product_id",
            how="inner"
        )

        fact_order_items = fact_order_items.withColumn(
            "line_amount",
            col("quantity") * col("unit_price")
        )

        # -------------------------
        # FACT REVIEWS
        # -------------------------
        fact_reviews = reviews_b.select(
            col("review_id").cast(IntegerType()).alias("review_id"),
            col("product_id").cast(IntegerType()).alias("product_id"),
            col("customer_id").cast(IntegerType()).alias("customer_id"),
            col("rating").cast(IntegerType()).alias("rating"),
            to_date(col("review_date")).alias("review_date")
        ).filter(
            (col("rating") >= 1) & (col("rating") <= 5)
        ).dropDuplicates(["review_id"])

        # FK validation reviews -> products & customers
        fact_reviews = fact_reviews.join(
            dim_products.select("product_id"),
            on="product_id",
            how="inner"
        ).join(
            dim_customers.select("customer_id"),
            on="customer_id",
            how="inner"
        )

        # -------------------------
        # DIM DATE  (IMPORTANT: date_key)
        # -------------------------
        dim_date = fact_orders.select(
            col("order_date").alias("date_key")
        ).dropDuplicates(["date_key"]) \
         .withColumn("year", year(col("date_key"))) \
         .withColumn("month", month(col("date_key"))) \
         .withColumn("day", dayofmonth(col("date_key"))) \
         .withColumn("week", weekofyear(col("date_key"))) \
         .withColumn("quarter", quarter(col("date_key")))

        # -------------------------
        # WRITE SILVER
        # -------------------------
        write_silver(dim_customers, "dim_customers", y, m, d)
        write_silver(dim_products, "dim_products", y, m, d)
        write_silver(dim_date, "dim_date", y, m, d)
        write_silver(fact_orders, "fact_orders", y, m, d)
        write_silver(fact_order_items, "fact_order_items", y, m, d)
        write_silver(fact_reviews, "fact_reviews", y, m, d)

        logging.info("===== SILVER PROCESSOR SUCCESS =====")

    except Exception as e:
        logging.error("SILVER PROCESSOR FAILED: {}".format(str(e)))
        raise

    finally:
        spark.stop()
        logging.info("Spark stopped")
        logging.info("===== END SILVER PROCESSOR =====")

# ====================================================
# ENTRY POINT
# ====================================================
if __name__ == "__main__":
    main()
