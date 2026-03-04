import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bronze Layer") \
    .config("spark.jars", "spark/mssql-jdbc-13.2.1.jre11.jar") \
    .getOrCreate()

jdbc_url = "jdbc:sqlserver://host.docker.internal:1434;databaseName=ecommerce;encrypt=false;trustServerCertificate=true"

jdbc_properties = {
    "user": "sa",
    "password": "Admin1234",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Odczytaj tabelę customers
df_customers = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.customers",
    properties=jdbc_properties
)

df_customers.show(5)
print(f"Liczba klientów: {df_customers.count()}")