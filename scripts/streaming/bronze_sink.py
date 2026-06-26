import pyspark
import os
from sparksession import create_spark_session

# create the spark session and configure the kafka connector
spark_version = pyspark.__version__

spark = create_spark_session(f"Bronze-Sink")
    
# hidden warnings (they are lame)
spark.sparkContext.setLogLevel("WARN")


### READ FROM KAFKA PIPELINE
kafka_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "receipts_flow") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 10000) \
    .load()
#startingOffsets =  latest  -> required for streaming data, otherwise we use earliest for batch. It tells us to read only new messages, ignoring the previous ones

## BRONZE LEVEL SINK -> Raw Data
bronze_data = kafka_data \
    .selectExpr("CAST(value AS STRING) as raw_json",
                "timestamp as kafka_arrival_time") # in this case we infer the missing timestamp as the time the data arrived from kafka

query_bronze = bronze_data.writeStream \
    .format("parquet") \
    .option("path", "s3a://retail.datalake/bronze/") \
    .option("checkpointLocation", "./checkpoints/bronze_test") \
    .trigger(processingTime="5 minutes") \
    .start()

query_bronze.awaitTermination()
# the checkpoint is used to remember always at what point of the computation we were when the system crush -> robustness