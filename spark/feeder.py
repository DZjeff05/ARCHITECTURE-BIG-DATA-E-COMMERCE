# -*- coding: utf-8 -*-

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# =========================
# CONFIG (Docker + HDFS)
# =========================
DATA_PATH = "/data"
RAW_PATH = "hdfs://namenode:9000/data/raw"   # IMPORTANT: host explicite

# =========================
# LOGGING
# =========================
LOG_DIR = "/project/logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "feeder.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def write_bronze(df, table_name, year, month, day):
    """
    Ajoute les colonnes de partition year/month/day puis écrit en Parquet dans HDFS.
    """
    df_out = (df
              .withColumn("year", lit(year))
              .withColumn("month", lit(month))
              .withColumn("day", lit(day)))

    output_path = "{}/{}".format(RAW_PATH, table_name)

    df_out.write \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)

    logging.info("Wrote BRONZE table {} to {}".format(table_name, output_path))


def main():
    logging.info("===== START BRONZE FEEDER =====")

    spark = SparkSession.builder \
        .appName("Bronze_Feeder_Ecommerce") \
        .getOrCreate()

    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    try:
        # -----------------------
        # Customers (CSV)
        # -----------------------
        logging.info("Reading customers.csv")
        customers_df = spark.read.option("header", True).csv(
            "{}/customers.csv".format(DATA_PATH)
        )
        write_bronze(customers_df, "customers", year, month, day)

        # -----------------------
        # Orders (CSV)
        # -----------------------
        logging.info("Reading orders.csv")
        orders_df = spark.read.option("header", True).csv(
            "{}/orders.csv".format(DATA_PATH)
        )
        write_bronze(orders_df, "orders", year, month, day)

        # -----------------------
        # Order Items (CSV)
        # -----------------------
        logging.info("Reading order_items.csv")
        order_items_df = spark.read.option("header", True).csv(
            "{}/order_items.csv".format(DATA_PATH)
        )
        write_bronze(order_items_df, "order_items", year, month, day)

        # -----------------------
        # Product Reviews (CSV)
        # -----------------------
        logging.info("Reading product_reviews.csv")
        reviews_df = spark.read.option("header", True).csv(
            "{}/product_reviews.csv".format(DATA_PATH)
        )
        write_bronze(reviews_df, "product_reviews", year, month, day)

        # -----------------------
        # Products (SQLite)
        # -----------------------
        logging.info("Reading products table from SQLite DB")
        products_df = spark.read.format("jdbc") \
            .option("url", "jdbc:sqlite:{}/products_data.db".format(DATA_PATH)) \
            .option("dbtable", "products") \
            .option("driver", "org.sqlite.JDBC") \
            .load()

        write_bronze(products_df, "products", year, month, day)

        logging.info("===== BRONZE FEEDER SUCCESS =====")

    except Exception as e:
        logging.error("BRONZE FEEDER FAILED: {}".format(str(e)))
        raise

    finally:
        spark.stop()
        logging.info("Spark session stopped")
        logging.info("===== END BRONZE FEEDER =====")


if __name__ == "__main__":
    main()
