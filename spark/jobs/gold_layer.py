"""
Gold layer: business-level aggregates read from Silver (Delta).

Tables produced:
  - revenue_by_category      — przychód i liczba zamówień per kategoria produktu
  - orders_summary_by_status — rozkład zamówień per status (z % udziałem)
  - top_customers            — ranking klientów wg łącznych wydatków
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
logger = logging.getLogger("gold_layer")

MINIO_ENDPOINT = require_env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = require_env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = require_env("MINIO_SECRET_KEY")

SILVER_CUSTOMERS_PATH = "s3a://lakehouse/silver/customers"
SILVER_ORDERS_PATH    = "s3a://lakehouse/silver/orders"
SILVER_PRODUCTS_PATH  = "s3a://lakehouse/silver/products"

GOLD_REVENUE_BY_CATEGORY_PATH    = "s3a://lakehouse/gold/revenue_by_category"
GOLD_ORDERS_BY_STATUS_PATH       = "s3a://lakehouse/gold/orders_summary_by_status"
GOLD_TOP_CUSTOMERS_PATH          = "s3a://lakehouse/gold/top_customers"


def build_spark_session() -> SparkSession:
    """Create Spark session configured for S3A + Delta.

    Returns:
        Configured SparkSession with Delta Lake and S3A support.
    """
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
        SparkSession.builder.appName("Gold Layer")
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


def revenue_by_category(df_orders: DataFrame, df_products: DataFrame) -> DataFrame:
    """Calculate revenue metrics grouped by product category.

    Joins orders with products, filters only 'completed' orders,
    then aggregates per category.

    Args:
        df_orders: Silver orders DataFrame.
        df_products: Silver products DataFrame.

    Returns:
        DataFrame with columns:
            category, total_revenue, order_count, avg_order_value
    """
    df_result = (
        df_orders.alias("o")
        .filter(F.col("o.status") == "completed")
        .join(df_products.alias("p"), F.col("o.product_id") == F.col("p.product_id"))
        .groupBy(F.col("p.category"))
        .agg(
            F.sum(F.col("o.total_amount")).alias("total_revenue"),
            F.count(F.col("o.order_id")).alias("order_count"),
            F.avg(F.col("o.total_amount")).alias("avg_order_value"),
        )
    )
    return df_result


def orders_summary_by_status(df_orders: DataFrame) -> DataFrame:
    """Summarise order counts and revenue per order status, with percentage share.

    Args:
        df_orders: Silver orders DataFrame.

    Returns:
        DataFrame with columns:
            status, order_count, total_revenue, pct_of_total
    """
    # TODO: napisz logikę:
    # 1. GROUP BY status → order_count, total_revenue
    # 2. Oblicz pct_of_total = order_count / suma_wszystkich * 100
    #    Wskazówka: możesz użyć Window bez partitionBy() albo df_orders.count()
    df_result = (

        df_orders.alias("o")
        .groupBy(F.col("o.status"))
        .agg(
            F.count(F.col("o.order_id")).alias("order_count"),
            F.sum(F.col("o.total_amount")).alias("total_revenue")
        )
        .withColumn(
            "pct_of_total",
            F.round(F.col("order_count") / F.sum("order_count").over(Window.partitionBy()) * 100, 2),
        )
    )


    return df_result


def top_customers(df_orders: DataFrame, df_customers: DataFrame) -> DataFrame:
    """Rank customers by total amount spent across all their orders.

    Args:
        df_orders: Silver orders DataFrame.
        df_customers: Silver customers DataFrame.

    Returns:
        DataFrame with columns:
            customer_id, full_name, country,
            total_spent, order_count, rank
    """
    # TODO: napisz logikę:
    # 1. GROUP BY customer_id na df_orders → total_spent, order_count
    # 2. JOIN z df_customers po customer_id
    # 3. Dodaj full_name = concat(first_name, ' ', last_name)
    # 4. Dodaj rank = DENSE_RANK() OVER (ORDER BY total_spent DESC)


    df_result = (
        df_orders.alias("o")
        .groupBy(F.col("o.customer_id"))
        .agg(
            F.sum(F.col("o.total_amount")).alias("total_spent"),
            F.count(F.col("o.order_id")).alias("order_count")
        )
        .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"))
        .withColumn("full_name", F.concat(F.col("c.first_name"), F.lit(" "), F.col("c.last_name")))
        .withColumn("rank", F.dense_rank().over(Window.orderBy(F.col("total_spent").desc())))
        .select("c.customer_id", "full_name", "country", "total_spent", "order_count", "rank")
    )
    return df_result


def write_gold(df: DataFrame, path: str) -> None:
    """Write a Gold aggregate DataFrame as a Delta table (no partitioning).

    Gold tables are small aggregates — partitioning is unnecessary overhead.

    Args:
        df: Transformed Gold DataFrame.
        path: Target S3A path for the Delta table.
    """
    # TODO: zapisz df jako Delta w trybie overwrite pod podaną ścieżką
    # (Gold zawsze nadpisujemy w całości — to są agregaty, nie surowe dane)
    df.write.format("delta").mode("overwrite").save(path)


def main() -> None:
    """Entrypoint: build all three Gold aggregates from Silver."""

    spark = build_spark_session()
    logger.info("Spark version: %s", spark.version)

    # Wczytaj tabele Silver (format Delta, nie Parquet!)
    df_customers = spark.read.format("delta").load(SILVER_CUSTOMERS_PATH)
    df_orders    = spark.read.format("delta").load(SILVER_ORDERS_PATH)
    df_products  = spark.read.format("delta").load(SILVER_PRODUCTS_PATH)

    logger.info("Silver customers count=%s", df_customers.count())
    logger.info("Silver orders count=%s",    df_orders.count())
    logger.info("Silver products count=%s",  df_products.count())

    # ── revenue_by_category ──────────────────────────────────────────────────
    # TODO: wywołaj revenue_by_category(), zaloguj count, pokaż wynik, zapisz
    df_result = revenue_by_category(df_orders, df_products)
    logger.info("Revenue by category count=%s", df_result.count())
    df_result.show()
    write_gold(df_result, GOLD_REVENUE_BY_CATEGORY_PATH)
    logger.info("Revenue by category written to %s", GOLD_REVENUE_BY_CATEGORY_PATH)

    # ── orders_summary_by_status ─────────────────────────────────────────────
    # TODO: wywołaj orders_summary_by_status(), zaloguj count, pokaż wynik, zapisz
    df_result = orders_summary_by_status(df_orders)
    logger.info("Orders summary by status count=%s", df_result.count())
    df_result.show()
    write_gold(df_result, GOLD_ORDERS_BY_STATUS_PATH)
    logger.info("Orders summary by status written to %s", GOLD_ORDERS_BY_STATUS_PATH)
    # ── top_customers ────────────────────────────────────────────────────────
    # TODO: wywołaj top_customers(), zaloguj count, pokaż wynik, zapisz
    df_result = top_customers(df_orders, df_customers)
    logger.info("Top customers count=%s", df_result.count())
    df_result.show()
    write_gold(df_result, GOLD_TOP_CUSTOMERS_PATH)
    logger.info("Top customers written to %s", GOLD_TOP_CUSTOMERS_PATH)


if __name__ == "__main__":
    main()
