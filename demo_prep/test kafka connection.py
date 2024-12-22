# Databricks notebook source
kafka_bootstrap_server = ""
security_protocol = "SASL_SSL"
sasl_mechanisms = "PLAIN"
sasl_username = ""
sasl_password= ""
topic = f"test_connection"

table_test_catalog = "kafka_demo_catalog"
table_test_schema = "kafka_demo_schema"
table_test_name = f"{table_test_catalog}.{table_test_schema}.connectivity_test"

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
user = username.split("@")[0].replace(".","_")
project_dir = f"/home/{username}/kafka_connectivity_test"
checkpoint_location = f"{project_dir}/kafka_checkpoint"
checkpoint_delta_location = f"{project_dir}/kafka_delta_checkpoint"
checkpoint_delta_silver = f"{project_dir}/kafka_delta_silver"
checkpoint_delta_gold = f"{project_dir}/kafka_delta_gold"

# Clear checkpoint location
dbutils.fs.rm(checkpoint_location, True)
dbutils.fs.rm(checkpoint_delta_location, True)
dbutils.fs.rm(checkpoint_delta_silver, True)
dbutils.fs.rm(checkpoint_delta_gold, True)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {table_test_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_test_catalog}.{table_test_schema}")

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import random, string, uuid

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

# COMMAND ----------

# LOADING STREAMING DATASET

input_path = "/databricks-datasets/structured-streaming/events"
input_schema = spark.read.json(input_path).schema

input_stream = (spark
  .readStream
  .schema(input_schema)
  .json(input_path)
  .withColumn("processingTime", lit(datetime.now().timestamp()).cast("timestamp"))
  .withColumn("eventId", uuidUdf()))

# COMMAND ----------

# WRITE TO KAFKA

(input_stream
   .select(col("eventId").alias("key"), to_json(struct(col('action'), col('time'), col('processingTime'))).alias("value"))
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", kafka_bootstrap_server )
   .option("kafka.sasl.mechanism", sasl_mechanisms)
   .option("kafka.security.protocol", security_protocol)
   .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{sasl_username}' password='{sasl_password}';")
   .option("checkpointLocation", checkpoint_location )
   .option("topic", topic)
   .trigger(availableNow=True)
   .start()
)

# COMMAND ----------

# READ FROM KAFKA TOPIC AND WRITE TO DELTA

query = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_server )
  .option("kafka.sasl.mechanism", sasl_mechanisms)
  .option("kafka.security.protocol", security_protocol)
  .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{sasl_username}' password='{sasl_password}';")
  .option("subscribe", topic )
  .option("startingOffsets", "earliest" )
  .load()
  .select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), input_schema).alias("json"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_delta_location)
  .outputMode("append")
  .trigger(availableNow=True)
  .table(table_test_name))

# COMMAND ----------

display(spark.table(table_test_name))

# COMMAND ----------


