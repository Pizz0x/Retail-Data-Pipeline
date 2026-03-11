from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# create the spark session and configure the kafka connector
spark = SparkSession.builder \
    .appName("FraudDetector") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# hidden warnings (they are lame)
spark.sparkContext.setLogLevel("WARN")

# structure of the data, we need to specify it cause Spark is not able to infer the correct types in case of continuos streaming of data
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True),
])

# get data from the Kafka pipeline
kafka_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_transactions") \
    .option("startingOffsets", "latest") \
    .load()
#startingOffsets =  latest  -> required for streaming data, otherwise we use earliest for batch. It tells us to read only new messages, ignoring the previous ones

# transform data from the encoding obtained from kafka to actually readable data -> first we convert data to string format, then we apply the wanted JSON schema and finally we expand columns
data = kafka_data \
    .selectExpr( "CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") 

query = data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()