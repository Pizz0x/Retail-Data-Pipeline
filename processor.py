import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, CharType, IntegerType, TimestampType, ArrayType

# create the spark session and configure the kafka connector
spark_version = pyspark.__version__
spark = SparkSession.builder \
    .appName("FraudDetector") \
    .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}") \
    .getOrCreate()

# hidden warnings (they are lame)
spark.sparkContext.setLogLevel("WARN")

### DEFINITION OF DATA SCHEMA -> we need to specify it cause Spark is not able to infer the correct types in case of continuos streaming of data

# structure of items data
item_schema = StructType([
    StructField("category", StringType(), True),
    StructField("model", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("sex", CharType(), True),
    StructField("size", StringType(), True),
])

# stucture of receipts data
receipt_schema = StructType([
    StructField("receipt_id", StringType(), True),
    StructField("store", StringType(), True),
    StructField("checkout", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("items", ArrayType(item_schema), True)
])


### READ FROM KAFKA PIPELINE
kafka_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "receipts_flow") \
    .option("startingOffsets", "latest") \
    .load()
#startingOffsets =  latest  -> required for streaming data, otherwise we use earliest for batch. It tells us to read only new messages, ignoring the previous ones


### PARSING AND TRANSFORMATION OF DATA
# transform data from the encoding obtained from kafka to actually readable data -> first we convert data to string format, then we apply the wanted JSON schema and finally we expand columns
receipt_data = kafka_data \
    .selectExpr( "CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), receipt_schema).alias("data")) \
    .select("data.*") 
# at this point we want to transform the list of items contained in the receipts in a list of individual items for the analysis of the sells
item_data = receipt_data \
    .select(
        "receipt_id",
        "store_city",
        "checkout",
        "timestamp",
        explode(col("items")).alias("individual_article") # create a row for every article
    ) \
    .select(
        "receipt_id",
        "store_city",
        "checkout",
        "timestamp",
        col("individual_article.category").alias("category"),
        col("individual_article.model").alias("model"),
        col("individual_article.prize").alias("prize"),
        col("individual_article.sex").alias("sex"),
        col("individual_article.size").alias("size")
    )


### DATA CLEANING
#todo


### WRITE IN CONSOLE
query = item_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()