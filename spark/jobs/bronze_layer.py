import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, year, month, dayofmonth
spark = SparkSession.builder \
    .appName("Bronze Layer") \
    .config("spark.jars", "spark/mssql-jdbc-13.2.1.jre11.jar,spark/hadoop-aws-3.3.4.jar,spark/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

jdbc_url = "jdbc:sqlserver://host.docker.internal:1434;databaseName=ecommerce;encrypt=false;trustServerCertificate=true"

jdbc_properties = {
    "user": "sa",
    "password": "Admin1234",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df_customers = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.customers",
    properties=jdbc_properties
)

df_orders = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.orders",
    properties=jdbc_properties
)

df_products = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.products",
    properties=jdbc_properties
)

df_customers = df_customers \
    .withColumn("ingestion_date", current_date()) \
    .withColumn("year", year(current_date())) \
    .withColumn("month", month(current_date())) \
    .withColumn("day", dayofmonth(current_date()))

df_orders = df_orders \
    .withColumn("ingestion_date", current_date()) \
    .withColumn("year", year(current_date())) \
    .withColumn("month", month(current_date())) \
    .withColumn("day", dayofmonth(current_date()))

df_products = df_products \
    .withColumn("ingestion_date", current_date()) \
    .withColumn("year", year(current_date())) \
    .withColumn("month", month(current_date())) \
    .withColumn("day", dayofmonth(current_date()))


df_customers.show(5)
print(f"Liczba klientów: {df_customers.count()}")

df_orders.show(5)
print(f"Liczba zamowien: {df_orders.count()}")

df_products.show(5)
print(f"Liczba produktow: {df_products.count()}")

# Zapisz do MinIO - Bronze layer
df_customers.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://lakehouse/bronze/customers/")

df_orders.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://lakehouse/bronze/orders/")

df_products.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://lakehouse/bronze/products/")

print("Zapisano customers do MinIO!")