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
model_serving_endpoint_name = "test_wine_mode_endpoint"

# Datasets
model_inference_input_table = "prod.default.model_inference_test"
model_inference_output_table = "prod.default.model_inference_test_result"

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
checkpoint_kafka_load_source_location = f"{project_dir}/kafka_load_source"
checkpoint_kafka_sync_location = f"{project_dir}/kafka_sync"

# COMMAND ----------

# Clear checkpoint location so that every time this notebook runs, there will be new data to be processed by the streaming jobs
dbutils.fs.rm(checkpoint_kafka_load_source_location, True)
#dbutils.fs.rm(checkpoint_kafka_sync_location, True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write test events to a Kafka topic

# COMMAND ----------

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

# Load test data
load_data_df = spark.readStream \
                  .table(model_inference_input_table) \
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
st_1 = load_data_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server ) \
    .option("kafka.sasl.mechanism", sasl_mechanisms) \
    .option("kafka.security.protocol", security_protocol) \
    .option("kafka.sasl.jaas.config", sasl_jaas_config) \
    .option("checkpointLocation", checkpoint_kafka_load_source_location ) \
    .option("topic", source_topic) \
    .trigger(availableNow=True) \
    .start()

st_1.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reads from Kafka and writes to different targets

# COMMAND ----------

def batch_inference(batch_df, batch_id):

  # Score batch
  batch_df_pd = batch_df.toPandas()
  dataframe_split=DataframeSplitInput.from_dict(batch_df_pd.to_dict(orient='split'))
  w = WorkspaceClient()
  response = w.serving_endpoints.query(name = model_serving_endpoint_name, dataframe_split = dataframe_split)
  batch_df_pd["good_quality_prediction"] = ["yes" if b else "no" for b in response.predictions]

  # Convert from pandas to Spark
  spark_df = spark.createDataFrame(batch_df_pd)

  # Write to Delta
  spark_df.withColumn("batch_id", lit(batch_id)).write.mode("append").saveAsTable(model_inference_output_table)

  # Write to Kafka
  spark_df \
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
      .write \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_bootstrap_server ) \
      .option("kafka.sasl.mechanism", sasl_mechanisms) \
      .option("kafka.security.protocol", security_protocol) \
      .option("kafka.sasl.jaas.config", sasl_jaas_config) \
      .option("topic", target_topic) \
      .save()


# COMMAND ----------

schema = spark.table(model_inference_input_table).schema

st_2 = spark.readStream \
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
  .option("checkpointLocation", checkpoint_kafka_sync_location ) \
  .foreachBatch(batch_inference) \
  .trigger(availableNow=True) \
  .start()

st_2.awaitTermination()

# COMMAND ----------


