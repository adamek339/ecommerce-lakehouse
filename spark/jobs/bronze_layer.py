import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from load_repo_env import bootstrap, require_env

REPO_ROOT = bootstrap()

import os

import logging
import uuid

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, dayofmonth, lit, month, year

minio_endpoint = require_env("MINIO_ENDPOINT")
access_key = require_env("MINIO_ACCESS_KEY")
secret_key = require_env("MINIO_SECRET_KEY")

jdbc_host = os.environ.get("MSSQL_JDBC_HOST", "").strip() or require_env("MSSQL_HOST")
jdbc_port = require_env("MSSQL_PORT")
jdbc_db = require_env("MSSQL_DATABASE")
jdbc_user = require_env("MSSQL_USER")
jdbc_password = require_env("MSSQL_PASSWORD")

_jar_dir = REPO_ROOT / "spark"
_jars = ",".join(
    str(_jar_dir / name)
    for name in (
        "mssql-jdbc-13.2.1.jre11.jar",
        "hadoop-aws-3.3.4.jar",
        "aws-java-sdk-bundle-1.12.262.jar",
    )
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("bronze_layer")

spark = (
    SparkSession.builder.appName("Bronze Layer")
    .config("spark.jars", _jars)
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

jdbc_url = (
    f"jdbc:sqlserver://{jdbc_host}:{jdbc_port};"
    f"databaseName={jdbc_db};encrypt=false;trustServerCertificate=true"
)

jdbc_properties = {
    "user": jdbc_user,
    "password": jdbc_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

df_customers = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.customers",
    properties=jdbc_properties,
)

df_orders = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.orders",
    properties=jdbc_properties,
)

df_products = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.products",
    properties=jdbc_properties,
)

run_id = str(uuid.uuid4())
ingestion_ts = current_timestamp()

df_customers = df_customers.withColumn("ingestion_date", current_date()).withColumn(
    "year", year(current_date())
).withColumn("month", month(current_date())).withColumn("day", dayofmonth(current_date()))
df_customers = df_customers.withColumn("ingestion_timestamp", ingestion_ts).withColumn("run_id", lit(run_id))

df_orders = df_orders.withColumn("ingestion_date", current_date()).withColumn(
    "year", year(current_date())
).withColumn("month", month(current_date())).withColumn("day", dayofmonth(current_date()))
df_orders = df_orders.withColumn("ingestion_timestamp", ingestion_ts).withColumn("run_id", lit(run_id))

df_products = df_products.withColumn("ingestion_date", current_date()).withColumn(
    "year", year(current_date())
).withColumn("month", month(current_date())).withColumn("day", dayofmonth(current_date()))
df_products = df_products.withColumn("ingestion_timestamp", ingestion_ts).withColumn("run_id", lit(run_id))

df_customers.show(5)
logger.info("customers read count=%s", df_customers.count())

df_orders.show(5)
logger.info("orders read count=%s", df_orders.count())

df_products.show(5)
logger.info("products read count=%s", df_products.count())

# Zapisz do MinIO - Bronze layer
df_customers.write.mode("append").partitionBy("year", "month", "day").parquet(
    "s3a://lakehouse/bronze/customers/"
)

df_orders.write.mode("append").partitionBy("year", "month", "day").parquet(
    "s3a://lakehouse/bronze/orders/"
)

df_products.write.mode("append").partitionBy("year", "month", "day").parquet(
    "s3a://lakehouse/bronze/products/"
)

logger.info("Bronze write completed (customers/orders/products appended).")
