import pyspark
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
from sparksession import create_spark_session

# search for the .env file and load the variables in the script

# create the spark session and configure the kafka connector
spark_version = pyspark.__version__

spark = create_spark_session(f"Silver-Sink")
 

# hidden warnings (they are lame)
spark.sparkContext.setLogLevel("WARN")

### DEFINITION OF DATA SCHEMA -> we need to specify it cause Spark is not able to infer the correct types in case of continuos streaming of data


# stucture of receipts data
silver_schema = StructType([
    StructField("receipt_id", StringType(), True),
    StructField("store", StringType(), True),
    StructField("checkout", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("payment", StringType(), True),
    StructField("test", BooleanType(), True),
    StructField("category", StringType(), True),
    StructField("model", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("sex", StringType(), True),
    StructField("size", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("square_footage", IntegerType(), True),
    StructField("loc_type", StringType(), True),
    StructField("list_price", DoubleType(), True),
    StructField("supplier", StringType(), True),
    StructField("cost", DoubleType(), True),
    StructField("sustainable", BooleanType(), True),
    StructField("checkout_type", StringType(), True),
    StructField("checkout_department", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("discount", DoubleType(), True),
    StructField("net_profit", DoubleType(), True),
    StructField("hour_of_day", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("day", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True),
])




### READ FROM KAFKA PIPELINE
kafka_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "silver_data") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 10000) \
    .load()

engineered_data = kafka_data \
    .select(from_json(col("value").cast("string"), silver_schema).alias("data")) \
    .select("data.*") \

### SILVER LEVEL SINK -> cleaned and processed data 
# for now we just write on console to check everything works fine
query_silver = engineered_data.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .partitionBy("year", "month", "day") \
    .option("path", "s3a://retail.datalake/silver/receipts/") \
    .option("checkpointLocation", "./checkpoints/silver/") \
    .trigger(processingTime="10 minutes") \
    .start()

query_silver.awaitTermination()