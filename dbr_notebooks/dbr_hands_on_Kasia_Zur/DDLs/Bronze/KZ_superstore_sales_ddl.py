# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    BooleanType,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
)
from pyspark.sql.functions import (
    current_timestamp,
    date_format,
)

# COMMAND ----------

display(spark.sql("SHOW CATALOGS"))


# COMMAND ----------

catalog_name = "dbr_dev_kasia_zur"
schema_name = "Bronze"
table_name = "KZ_superstore_sales"
target_path = "s3://dbr-infoshare-dev-ue-west-one-training/Trening_DBR/Kasia_Zur/Bronze"
# source_path = "s3://dbr-infoshare-dev-ue-west-one-training/Trening_DBR/Source/Sample - Superstore.csv"


# === STWORZENIE SCHEMATU ===
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# === USUNIĘCIE TABELI JEŚLI ISTNIEJE ===
spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{table_name}")




# COMMAND ----------

# === TWORZENIE TABELI ===
spark.sql(f"""
CREATE EXTERNAL TABLE {catalog_name}.{schema_name}.{table_name} (
  _load_timestamp_bronze STRING NOT NULL,
  `Row_ID` STRING,
  `Order_ID` STRING,
  `Order_Date` STRING,
  `Ship_Date` STRING,
  `Ship_Mode` STRING,
  `Customer_ID` STRING,
  `Customer_Name` STRING,
  `Segment` STRING,
  `Country` STRING,
  `City` STRING,
  `State` STRING,
  `Postal_Code` STRING,
  `Region` STRING,
  `Product_ID` STRING,
  `Category` STRING,
  `Sub_Category` STRING,
  `Product_Name` STRING,
  `Sales` STRING,
  `Quantity` STRING,
  `Discount` STRING,
  `Profit` STRING
)
USING DELTA
PARTITIONED BY (_load_timestamp_bronze)
LOCATION '{target_path}'
TBLPROPERTIES (
  'delta.appendOnly' = 'true'
)
COMMENT 'Tabela bronze - dane sprzedażowe superstore w formie surowej (stringi)'
""")

# === USTAWIENIE TAGA ===
spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} SET TAGS ('layer' = 'bronze')")

# === NADANIE UPRAWNIEŃ ===
spark.sql(f"GRANT SELECT ON TABLE {catalog_name}.{schema_name}.{table_name} TO `der1-kursanci`")

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED dbr_dev_kasia_zur.bronze.kz_superstore_sales

# COMMAND ----------

## Wczytanie danych z csv

from datetime import datetime
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType

# === PARAMETRY ===
catalog_name = "dbr_dev_kasia_zur"
schema_name = "bronze"
table_name = "kz_superstore_sales"
source_path = "s3://dbr-infoshare-dev-ue-west-one-training/Trening_DBR/Source/Sample - Superstore.csv"

target_table = f"{catalog_name}.{schema_name}.{table_name}"

# === SCHEMAT WSZYSTKIE STRINGI ===
schema = StructType([
    StructField("Row_ID", StringType()),
    StructField("Order_ID", StringType()),
    StructField("Order_Date", StringType()),
    StructField("Ship_Date", StringType()),
    StructField("Ship_Mode", StringType()),
    StructField("Customer_ID", StringType()),
    StructField("Customer_Name", StringType()),
    StructField("Segment", StringType()),
    StructField("Country", StringType()),
    StructField("City", StringType()),
    StructField("State", StringType()),
    StructField("Postal_Code", StringType()),
    StructField("Region", StringType()),
    StructField("Product_ID", StringType()),
    StructField("Category", StringType()),
    StructField("Sub_Category", StringType()),
    StructField("Product_Name", StringType()),
    StructField("Sales", StringType()),
    StructField("Quantity", StringType()),
    StructField("Discount", StringType()),
    StructField("Profit", StringType())
])

# === DODANIE ZNACZNIKA CZASU ===
load_ts = datetime.now().strftime("%Y%m%d%H%M%S")

df = (
    spark.read.schema(schema)
    .option("header", True)
    .csv(source_path)
    .withColumn("_load_timestamp_bronze", lit(load_ts))
)

# === ZAPIS DO TABELI ===
df.write.format("delta")\
  .mode("append")\
  .partitionBy("_load_timestamp_bronze")\
  .saveAsTable(target_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbr_dev_kasia_zur.bronze.kz_superstore_sales LIMIT 10
