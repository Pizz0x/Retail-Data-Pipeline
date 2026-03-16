import pyspark
from pyspark.sql import SparkSession
from functools import reduce
from operator import or_
from pyspark.sql.functions import from_json, col, explode, broadcast, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType, BooleanType

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
    StructField("sex", StringType(), True),
    StructField("size", StringType(), True),
    StructField("quantity", IntegerType(), True),
])

# stucture of receipts data
receipt_schema = StructType([
    StructField("receipt_id", StringType(), True),
    StructField("store", StringType(), True),
    StructField("checkout", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("test", BooleanType(), True),
    StructField("items", ArrayType(item_schema), True)
])

### LOADING STATIC TABLES -> they are then used for data enrichment
# we will say header=true to declare that the name of the columns should be the one given in the csv file
# we will say inferschema=true to declare that we want the schema to be inferred from data (otherwise it's all strings), we cannot do so in Structured Streaming since Spark cannot look at all the data and decide (they keep flowinf)

df_stores = spark.read \
    .option("header", "true") \
    .option("InferSchema", "true") \
    .csv("./data/stores.csv")

df_checkouts = spark.read \
    .option("header", "true") \
    .option("InferSchema", "true") \
    .csv("./data/checkouts.csv")

df_items = spark.read \
    .option("header", "true") \
    .option("InferSchema", "true") \
    .csv("./data/items.csv")


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
        "store",
        "checkout",
        "timestamp",
        "test",
        explode(col("items")).alias("individual_article") # create a row for every article
    ) \
    .select(
        "receipt_id",
        "store",
        "checkout",
        "timestamp",
        "test",
        col("individual_article.category").alias("category"),
        col("individual_article.model").alias("model"),
        col("individual_article.price").alias("price"),
        col("individual_article.sex").alias("sex"),
        col("individual_article.size").alias("size"),
        col("individual_article.size").alias("quantity")
    )

### DATA CLEANING
# handle null values by adding a new feauture that reports problematic instances 
# we divide data in 2 flows -> one for the signaled data (that goes into a log file) and the other with the data that pass the check
critical_fields = ["receipt_id", "store", "price", "category", "model"]
critical_condition = reduce(or_, [col(c).isNull() for c in critical_fields])

important_fields = ["checkout", "timestamp", "quantity"] # field that we have to handle by putting default values
informative_fields = ["total_amount", "sex", "size"] # filed that we have to handle by just setting them as N/A

tagget_data = item_data.withColumn(
        "error",
        when(col("test")==True, "TEST_TRANSACTION")
        .when(critical_condition, "MISSING_CRITICAL_FIELD")
        .otherwise(None)
    )
# we then throw problematic instances straight to the log queue and we remove them from the data to be processed
log_struct = tagget_data.filter(col("error").isNotNull()) 
item_data = tagget_data.filter(col("error").isNull()).drop("error")

### DATA ENRICHMENT
# we want to add information to the rows by exploiting already known things about data that are not automatically added by the checkout. Indeed adding all data directly from the checkout is less realistic and it means more data to send through the pipeline (less efficient)
# since we have small static tables with the additional informations, in the case of streaming of data, the more convenient thing to do is doing broadcast (we pass the small tables to each executor, way more efficient)
enriched_data = item_data.join(
        broadcast(df_stores),
        on="store",
        how="left"  # this way if the store is not in the static table, we don't lose the receipt
    ). \
    join(
        broadcast(df_items),
        on=["category","model"],
        how="left"
    ). \
    join(
        broadcast(df_checkouts),
        on=["store", "checkout"],
        how="left"
    )

### DATA ENGINEERING
# we now compute a further step in data cleaning by handling of non-crytical features null values, returns of clothes, absurd prices and duplicates
# we put in the log file, rows with problematic prices and for what concerns with other problems we deal with them with fallback values 
validated_data = enriched_data.withColumn(
        "error",
        when(col("price") > col("list_price"), f"PRICE_EXCEEDS_CATALOG")
        .when(col("price") < (-1*col("list_price")), "REFUND_EXCEEDS_CATALOG")
        .otherwise(None)
    )

# split in log data and correct data
log_price = validated_data.filter(col("error").isNotNull())
enriched_data = validated_data.filter(col("error").isNull()).drop("error")
# now we are ensured that the data are correct and we can proceed with the data engineering part

# first we are gonna deal with NaN values for the important / informative values


# the we are gonna add some new informative fields:
# - a new boolean field that flags if an item is a sale or a return
# - if there is a discout applied and of how much
# - the profit in the sale of the article
# - the margin (profit / list_price) * 100
# - temporal information (hour of the day) (season) (month) (day of the week)


# create new feautures based on the given ones (discount = list_price-price, profit = price-cost), maybe creating an amount feature and remove the rows of the same receipt with the same clothes

### STATEFUL AGGREGATIONS
#todo


### SINK
# for now we just write on console to check everything works fine
query = enriched_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()