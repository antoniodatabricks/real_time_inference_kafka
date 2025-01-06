# Databricks notebook source
# MAGIC %md
# MAGIC # Parameters and variables

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import DataframeSplitInput
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import uuid

# COMMAND ----------

# Model Serving Endpoint
model_serving_endpoint_name = ""

# Datasets
test_source_table = "" # Table containing the test data to be fed to Kafka to simulate source records 
model_inference_output_table = "" # Delta table where the real-time inference output will be written to

# Kafka connection info
kafka_bootstrap_server = ""
security_protocol = "SASL_SSL"
sasl_mechanisms = "PLAIN"
sasl_jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='' password='';"
source_topic = ""
target_topic = ""

# Streaming variables
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
project_dir = f"/home/{username}/kafka_test"
checkpoint_kafka_delta_sync = f"{project_dir}/kafka_delta_sync/{target_topic}"
checkpoint_delta_kafka_sync = f"{project_dir}/delta_kafka_sync/{target_topic}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Write test events to a Kafka topic

# COMMAND ----------

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())
schema = spark.table(test_source_table).schema

# Load test data
load_data_df = spark.read \
                  .table(test_source_table) \
                  .withColumn("processingTime", lit(datetime.now().timestamp()).cast("timestamp")) \
                  .withColumn("eventId", uuidUdf()) \
                  .select(col("eventId").alias("key"), to_json(struct(col('fixed_acidity'), 
                                                                      col('volatile_acidity'),  
                                                                      col('citric_acid'),  
                                                                      col('residual_sugar'),  
                                                                      col('chlorides'),  
                                                                      col('free_sulfur_dioxide'),  
                                                                      col('total_sulfur_dioxide'),  
                                                                      col('density'),  
                                                                      col('pH'),  
                                                                      col('sulphates'), 
                                                                      col('alcohol'), 
                                                                      col('is_red'), 
                                                                      col('processingTime'))).alias("value"))

# COMMAND ----------

# Write test data to a Kafka topic
load_data_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server ) \
    .option("kafka.sasl.mechanism", sasl_mechanisms) \
    .option("kafka.security.protocol", security_protocol) \
    .option("kafka.sasl.jaas.config", sasl_jaas_config) \
    .option("topic", source_topic) \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Scoring using model a serving endpoint
# MAGIC
# MAGIC - If you need to write the output of a streaming query to multiple locations, Databricks recommends using multiple Structured Streaming writers for best parallelization and throughput. In this case, the first streaming to writes to delta, and the second streaming writes to kafka.
# MAGIC
# MAGIC See https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/foreach#write-to-multiple-locations
# MAGIC

# COMMAND ----------

def batch_inference(batch_df, batch_id):

  # Score batch
  batch_df_pd = batch_df.toPandas()
  dataframe_split=DataframeSplitInput.from_dict(batch_df_pd.to_dict(orient='split'))
  w = WorkspaceClient()
  response = w.serving_endpoints.query(name = model_serving_endpoint_name, dataframe_split = dataframe_split)
  batch_df_pd["good_quality_prediction"] = ["yes" if b else "no" for b in response.predictions]

  # Write to Delta
  spark.createDataFrame(batch_df_pd).withColumn("batch_id", lit(batch_id)).write.mode("append").saveAsTable(model_inference_output_table)

# COMMAND ----------

# Read from Kafka and Write to Delta
spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_server ) \
  .option("kafka.sasl.mechanism", sasl_mechanisms) \
  .option("kafka.security.protocol", security_protocol) \
  .option("kafka.sasl.jaas.config", sasl_jaas_config) \
  .option("subscribe", source_topic ) \
  .option("startingOffsets", "earliest" ) \
  .load() \
  .select(from_json(col("value").cast("string"), schema).alias("data")) \
  .select("data.*") \
  .writeStream \
  .queryName("read_kafka_write_delta") \
  .option("checkpointLocation", checkpoint_kafka_delta_sync ) \
  .foreachBatch(batch_inference) \
  .trigger(availableNow=True) \
  .start() \
  .awaitTermination()

# COMMAND ----------

# Read from Delta and Write to Kafka
spark.readStream \
      .table(model_inference_output_table) \
      .withColumn("processingTime", lit(datetime.now().timestamp()).cast("timestamp")) \
      .withColumn("eventId", uuidUdf()) \
      .select(col("eventId").alias("key"), to_json(struct(col('fixed_acidity'), 
                                                          col('volatile_acidity'),  
                                                          col('citric_acid'),  
                                                          col('residual_sugar'),  
                                                          col('chlorides'),
                                                          col('free_sulfur_dioxide'),  
                                                          col('total_sulfur_dioxide'),  
                                                          col('density'),  
                                                          col('pH'),  
                                                          col('sulphates'), 
                                                          col('alcohol'), 
                                                          col('is_red'), 
                                                          col('good_quality_prediction'), 
                                                          col('processingTime'))).alias("value")) \
      .writeStream \
      .queryName("read_delta_write_kafka") \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_bootstrap_server ) \
      .option("kafka.sasl.mechanism", sasl_mechanisms) \
      .option("kafka.security.protocol", security_protocol) \
      .option("kafka.sasl.jaas.config", sasl_jaas_config) \
      .option("topic", target_topic) \
      .option("checkpointLocation", checkpoint_delta_kafka_sync ) \
      .trigger(availableNow=True) \
      .start() \
      .awaitTermination()
