"""
Silver layer: deduplicate customers, orders and products by natural key
using the latest ingestion_timestamp from Bronze.

Expected Bronze schema additions per table:
  - ingestion_timestamp  (set once per pipeline run)
  - run_id               (UUID shared across all tables in one run)
  - year / month / day   (partition columns, based on ingestion date)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from load_repo_env import bootstrap, require_env

REPO_ROOT = bootstrap()

import findspark

findspark.init()

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("silver_layer")

MINIO_ENDPOINT = require_env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = require_env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = require_env("MINIO_SECRET_KEY")

BRONZE_CUSTOMERS_PATH = "s3a://lakehouse/bronze/customers"
BRONZE_ORDERS_PATH = "s3a://lakehouse/bronze/orders"
BRONZE_PRODUCTS_PATH = "s3a://lakehouse/bronze/products"

SILVER_CUSTOMERS_PATH = "s3a://lakehouse/silver/customers"
SILVER_ORDERS_PATH = "s3a://lakehouse/silver/orders"
SILVER_PRODUCTS_PATH = "s3a://lakehouse/silver/products"


def build_spark_session() -> SparkSession:
    """Create Spark session configured for S3A + Delta."""

    _jar_dir = REPO_ROOT / "spark"
    _jars = ",".join(
        str(_jar_dir / name)
        for name in (
            "mssql-jdbc-13.2.1.jre11.jar",
            "hadoop-aws-3.3.4.jar",
            "aws-java-sdk-bundle-1.12.262.jar",
        )
    )

    builder = (
        SparkSession.builder.appName("Silver Layer")
        .config("spark.jars", _jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _deduplicate_latest(df: DataFrame, key_col: str, table_name: str) -> DataFrame:
    """Keep only the latest record per natural key (by ingestion_timestamp).

    Args:
        df: Bronze DataFrame with ingestion_timestamp column.
        key_col: Name of the natural/business key column (e.g. 'customer_id').
        table_name: Used only in error messages.

    Returns:
        DataFrame with exactly one row per key_col value (the latest ingested).

    Raises:
        RuntimeError: When required metadata columns are missing from Bronze.
    """
    required = {key_col, "ingestion_timestamp"}
    missing = required.difference(set(df.columns))
    if missing:
        raise RuntimeError(
            f"Bronze {table_name} is missing required columns: "
            f"{sorted(missing)}. Run `spark/jobs/bronze_layer.py` first."
        )

    # desc_nulls_last: rows without ingestion_timestamp (old Bronze files before
    # metadata was added) lose to any row that has a real timestamp.
    w = Window.partitionBy(key_col).orderBy(F.col("ingestion_timestamp").desc_nulls_last())
    return (
        df.withColumn("__rn", F.row_number().over(w))
        .filter(F.col("__rn") == F.lit(1))
        .drop("__rn")
    )


def _write_silver(df: DataFrame, path: str) -> None:
    """Write a deduplicated DataFrame to Silver as a partitioned Delta table.

    Args:
        df: Transformed Silver DataFrame.
        path: Target S3A path for the Delta table.
    """
    partition_cols = ["year", "month", "day"]
    writer = df.write.format("delta").mode("overwrite")
    if all(c in df.columns for c in partition_cols):
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


def main() -> None:
    """Entrypoint: process customers, orders and products into Silver."""

    spark = build_spark_session()
    logger.info("Spark version: %s", spark.version)

    # ── customers ────────────────────────────────────────────────────────────
    # mergeSchema=True handles mixed Bronze files: old files (before metadata
    # columns were added) lack ingestion_timestamp; Spark fills them with null.
    df_bronze_customers = spark.read.option("mergeSchema", "true").parquet(BRONZE_CUSTOMERS_PATH)
    logger.info("Bronze customers read count=%s", df_bronze_customers.count())

    df_silver_customers = _deduplicate_latest(df_bronze_customers, "customer_id", "customers")
    logger.info("Silver customers deduplicated count=%s", df_silver_customers.count())
    df_silver_customers.show(5)
    _write_silver(df_silver_customers, SILVER_CUSTOMERS_PATH)
    logger.info("Silver customers written to %s", SILVER_CUSTOMERS_PATH)

    # ── orders ───────────────────────────────────────────────────────────────
    df_bronze_orders = spark.read.option("mergeSchema", "true").parquet(BRONZE_ORDERS_PATH)
    logger.info("Bronze orders read count=%s", df_bronze_orders.count())

    df_silver_orders = _deduplicate_latest(df_bronze_orders, "order_id", "orders")
    logger.info("Silver orders deduplicated count=%s", df_silver_orders.count())
    df_silver_orders.show(5)
    _write_silver(df_silver_orders, SILVER_ORDERS_PATH)
    logger.info("Silver orders written to %s", SILVER_ORDERS_PATH)

    # ── products ─────────────────────────────────────────────────────────────
    df_bronze_products = spark.read.option("mergeSchema", "true").parquet(BRONZE_PRODUCTS_PATH)
    logger.info("Bronze products read count=%s", df_bronze_products.count())

    df_silver_products = _deduplicate_latest(df_bronze_products, "product_id", "products")
    logger.info("Silver products deduplicated count=%s", df_silver_products.count())
    df_silver_products.show(5)
    _write_silver(df_silver_products, SILVER_PRODUCTS_PATH)
    logger.info("Silver products written to %s", SILVER_PRODUCTS_PATH)


if __name__ == "__main__":
    main()
