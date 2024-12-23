# Databricks notebook source
# MAGIC %md ## Create Unity Catalog and Schemas

# COMMAND ----------

# Prod environment
PROD_CATALOG_NAME = "prod"
PROD_SCHEMA_NAME = "default"

# COMMAND ----------

# Prod
spark.sql(f"CREATE CATALOG IF NOT EXISTS {PROD_CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PROD_CATALOG_NAME}.{PROD_SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md ## Write data to Unity Catalog tables
# MAGIC The dataset is available in `databricks-datasets`. In the following cell, you read the data in from `.csv` files into Spark DataFrames. You then write the DataFrames to tables in Unity Catalog. This both persists the data and lets you control how to share it with others.

# COMMAND ----------

white_wine = spark.read.csv("/databricks-datasets/wine-quality/winequality-white.csv", sep=';', header=True)
red_wine = spark.read.csv("/databricks-datasets/wine-quality/winequality-red.csv", sep=';', header=True)

# Remove the spaces from the column names
for c in white_wine.columns:
    white_wine = white_wine.withColumnRenamed(c, c.replace(" ", "_"))
for c in red_wine.columns:
    red_wine = red_wine.withColumnRenamed(c, c.replace(" ", "_"))

# Define table names
red_wine_table = f"{PROD_CATALOG_NAME}.{PROD_SCHEMA_NAME}.red_wine"
white_wine_table = f"{PROD_CATALOG_NAME}.{PROD_SCHEMA_NAME}.white_wine"

# Write to tables in Unity Catalog
spark.sql(f"DROP TABLE IF EXISTS {red_wine_table}")
spark.sql(f"DROP TABLE IF EXISTS {white_wine_table}")
white_wine.write.saveAsTable(f"{PROD_CATALOG_NAME}.{PROD_SCHEMA_NAME}.white_wine")
red_wine.write.saveAsTable(f"{PROD_CATALOG_NAME}.{PROD_SCHEMA_NAME}.red_wine")

# COMMAND ----------

from pyspark.sql.functions import *

input_table_test = white_wine.withColumn("is_red", lit(0.0)).drop("quality")
input_table_test.write.mode("overwrite").saveAsTable(f"{PROD_CATALOG_NAME}.default.model_inference_test")
