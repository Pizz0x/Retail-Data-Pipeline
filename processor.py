import pyspark
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.window import Window
from operator import or_
from pyspark.sql.functions import from_json, col, explode, broadcast, when, current_timestamp, expr, abs as _abs, sum, count, hour, month, round, date_format, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType, BooleanType

# create the spark session and configure the kafka connector
spark_version = pyspark.__version__
spark_version = pyspark.__version__

spark = SparkSession.builder \
    .appName("RetailDataPipeline") \
    .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                                   f"org.apache.hadoop:hadoop-aws:3.3.4,"
                                   f"com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                                   f"com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.10.0,com.clickhouse:clickhouse-jdbc:0.9.5") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "15000") \
    .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.idle.time", "60000") \
    .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .config("spark.hadoop.fs.s3a.connection.ttl", "300000") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
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
    StructField("checkout", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("payment", StringType(), True),
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
    .option("maxOffsetsPerTrigger", 50000) \
    .load()
#startingOffsets =  latest  -> required for streaming data, otherwise we use earliest for batch. It tells us to read only new messages, ignoring the previous ones

### BRONZE LEVEL SINK -> Raw Data
bronze_data = kafka_data \
    .selectExpr("CAST(value AS STRING) as raw_json",
                "timestamp as kafka_arrival_time") # in this case we infer the missing timestamp as the time the data arrived from kafka

query_bronze = bronze_data.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://retail.datalake/bronze/receipts/") \
    .option("checkpointLocation", "s3a://retail.datalake/checkpoints/bronze/") \
    .start()
# the checkpoint is used to remember always at what point of the computation we were when the system crush -> robustness



### PARSING AND TRANSFORMATION OF DATA
# transform data from the encoding obtained from kafka to actually readable data -> first we convert data to string format, then we apply the wanted JSON schema and finally we expand columns
receipt_data = kafka_data \
    .selectExpr( "CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), receipt_schema).alias("data")) \
    .select("data.*") 

# we compute the total amount in case it is null, before splitting data in articles since in this way we can compute it efficientyl without shuffling data
# we also need to ensure that timestamp is not null for the next computation (the removal of duplicates with the watermark)
receipt_data = receipt_data.withColumn("timestamp",
        when(col("timestamp").isNull(), current_timestamp()).otherwise(col("timestamp"))
    ) \
    .withColumn(
        "total_price",
        when(
            col("total_price").isNull(),
            # Questa formula fa un loop interno all'array 'items' e somma price * quantity
            expr("aggregate(items, 0D, (acc, item) -> acc + (item.price * coalesce(item.quantity, 1)))")
        ).otherwise(col("total_price"))
)


# we also delete network duplicates here since after the splitting of data it would be more difficult cause they would be splitted around the multiple nodes and are more difficult to find, while now we just have to find 2 equal receipt_id to be sure there is a duplicate
# we will use a watermark to ensure the retrieval of receipt after at most 10 minutes, then they could even get lost (which is quite rare)
# without a watermark, Spark have to remember all the ids which is not feasible
receipt_data = receipt_data \
    .withWatermark("timestamp", "2 minutes") \
    .dropDuplicates(["receipt_id"])


# at this point we want to transform the list of items contained in the receipts in a list of individual items for the analysis of the sells
item_data = receipt_data \
    .select(
        "receipt_id",
        "store",
        "checkout",
        "timestamp",
        "total_price",
        "payment",
        "test",
        explode(col("items")).alias("individual_article") # create a row for every article
    ) \
    .select(
        "receipt_id",
        "store",
        "checkout",
        "timestamp",
        "total_price",
        "payment",
        "test",
        col("individual_article.category").alias("category"),
        col("individual_article.model").alias("model"),
        col("individual_article.price").alias("price"),
        col("individual_article.sex").alias("sex"),
        col("individual_article.size").alias("size"),
        col("individual_article.quantity").alias("quantity")
    )


### DATA CLEANING
# handle null values by adding a new feauture that reports problematic instances 
# we divide data in 2 flows -> one for the signaled data (that goes into a log file) and the other with the data that pass the check
critical_fields = ["receipt_id", "store", "price", "category", "model"]
critical_condition = reduce(or_, [col(c).isNull() for c in critical_fields])

important_fields = ["checkout", "timestamp", "quantity"] # field that we have to handle by putting default values
informative_fields = ["total_price", "sex", "size", "payment"] # filed that we have to handle by just setting them as N/A

tagget_data = item_data.withColumn(
        "error",
        when(col("test"), "TEST_TRANSACTION")
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
        when(col("list_price").isNull(), "ITEM_NOT_IN_CATALOG") # if in the enrichment table we still don't have the article, the broadcast will let it be using null values for the static table features, we have to flag it
        .when(col("price") > col("list_price"), f"PRICE_EXCEEDS_CATALOG")
        .when(col("price") < (-1*col("list_price")), "REFUND_EXCEEDS_CATALOG")
        .otherwise(None)
    )

# split in log data and correct data
log_price = validated_data.filter(col("error").isNotNull())
enriched_data = validated_data.filter(col("error").isNull()).drop("error")
# now we are ensured that the data are correct and we can proceed with the data engineering part

# first we are gonna deal with the remaining features in case of missing values
enriched_data = enriched_data.withColumn("quantity",
        when(col("quantity").isNull(), 1).otherwise(col("quantity"))
    )

enriched_data = enriched_data.fillna({
    "checkout": "UNKNOWN_CHECKOUT",
    "sex": "UNISEX",
    "size": "UNKNOWN",
    "payment": "UNKNOWN"
})

# now we can finally add some new informative fields:
# - a new boolean field that flags if an item is a sale or a return
# - if there is a discout applied and of how much
# - the profit in the sale of the article
# - the margin (profit / list_price) * 100
# - temporal information (hour of the day) (season) (month) (day of the week)
engineered_data = enriched_data.withColumn("transaction_type",
        when(col("price")<0, "RETURN").otherwise("SALE")
    ) \
    .withColumn("discount",
        round((col("list_price")-_abs(col("price"))) / col("list_price") * 100,2)
    ) \
    .withColumn("net_profit",
        when(
            col("transaction_type") == "SALE", 
            round((_abs(col("price")*col("quantity")) - col("cost"))*col("quantity"), 2)
        ).otherwise(
            round(-1 * (_abs(col("price")*col("quantity")) - col("cost"))*col("quantity"), 2)
        )
    )

engineered_data = engineered_data.withColumn(
    "hour_of_day", hour(col("timestamp"))
    ) \
    .withColumn(
        "day_of_week", date_format(col("timestamp"), "EEEE")
    ) \
    .withColumn(
        "month", month(col("timestamp"))
    )

### SILVER LEVEL SINK -> cleaned and processed data 
# for now we just write on console to check everything works fine
query_silver = engineered_data.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://retail.datalake/silver/receipts/") \
    .option("checkpointLocation", "s3a://retail.datalake/checkpoints/silver/") \
    .start()



### STATEFUL AGGREGATIONS

# check number of transactions for each type of payment in a given checkout / store
live_transactions = engineered_data \
    .groupBy(
        window(col("timestamp"), "2 minutes"),
        col("store"),
        col("checkout"),
        col("payment")
    ) \
    .agg(count("*").alias("transaction_number"))


# check the article that is being more sold and the profit that it gives in a store at the moment, at the same time check the return rate on the articles (if too high it means that the product has some kind of difects)
trending_article = engineered_data \
    .groupBy(
        window(col("timestamp"), "2 minutes", "1 minutes"),
        col("category"),
        col("model"),
        col("sex"),
        col("supplier"),
        col("store")
    ) \
    .agg(
        sum(when(col("transaction_type")=="SALE", col("quantity")).otherwise(0)).alias("sold_articles"),
        sum(col("net_profit")).alias("profit_articles"),  # profit is already computed on the quantity of articles sold and is negative in case of return
        sum(when(col("transaction_type")=="RETURN", col("quantity")).otherwise(0)).alias("return_articles"),
    ) \
    .withColumn(
        "return_rate",
        (col("return_articles") / (col("sold_articles")+col("return_articles")))*100
    )

# check the checkout and so even the store which is getting more profit and revenue at the moment, at the same moment we check the return rate (so that the manager can know if a cashier is a dodger)
store_checkout_stats = engineered_data \
    .groupBy(
        window(col("timestamp"), "3 minutes", "1 minutes"),
        col("store"),
        col("region"),
        col("loc_type"),
        col("square_footage"),
        col("checkout"),
        col("checkout_type"),
        col("checkout_department")
    ) \
    .agg(
        sum(col("net_profit")).alias("ck_net_profit"), # profit is already computed on the quantity of articles sold and is negative in case of return
        sum(when(col("transaction_type")=="SALE",col("price")*col("quantity"))
            .when(col("transaction_type")=="RETURN", -col("price")*col("quantity"))
            .otherwise(0)).alias("ck_profit"),
        sum(when(col("transaction_type")=="SALE",col("list_price")*col("quantity"))
            .when(col("transaction_type")=="RETURN", -col("list_price")*col("quantity"))
            .otherwise(0)).alias("ck_theoretic_profit"),
        sum(when(col("transaction_type")=="SALE",col("cost")*col("quantity"))
            .when(col("transaction_type")=="RETURN", -col("cost")*col("quantity"))
            .otherwise(0)).alias("ck_costs"),
        sum(when(col("transaction_type") == "SALE", col("quantity")).otherwise(0)).alias("ck_total_sales"),
        sum(when(col("transaction_type") == "RETURN", col("quantity")).otherwise(0)).alias("ck_total_return"),
        sum(when(col("transaction_type") == "SALE", col("discount")).otherwise(0)).alias("total_discount"), # this has then to be divided by the quantity on the interface platform that does the graphics (if we do the average directly here for the checkout then it would not be possible to do so even for the stores)
    ) \
    .withColumn(
        "ck_return_rate",
        (col("ck_total_return") / (col("ck_total_return")+col("ck_total_sales")))*100
    ) \
    .withColumn(
        "ck_net_margin",
        ((col("ck_profit") - col("ck_costs"))/ col("ck_theoretic_profit")) * 100
    )

store_checkout_stats = store_checkout_stats.select(
    col("store"),
    col("checkout"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("region"),
    col("loc_type"),
    col("square_footage"),
    col("checkout_type"),
    col("checkout_department"),
    col("ck_net_profit"),
    col("ck_profit"),
    col("ck_theoretic_profit"),
    col("ck_costs"),
    col("ck_total_sales"),
    col("ck_total_return"),
    col("total_discount"),
    col("ck_return_rate"),
    col("ck_net_margin")
)

# function to write batch in the databases
def write_on_clickhouse(df_batch, epoch_id):
    df_batch.write \
        .format("clickhouse") \
        .option("host", "localhost") \
        .option("port", "8123") \
        .option("user", "default") \
        .option("password", "clickhouse123") \
        .option("database", "retail_stats") \
        .option("table", "checkout_analytics") \
        .mode("append") \
        .save()
    
store_checkout_query = store_checkout_stats.writeStream \
    .outputMode("append") \
    .foreachBatch(write_on_clickhouse) \
    .option("checkpointLocation", "s3a://retail.datalake/checkpoints/clickhouse_gold/") \
    .start()

# then for the analysis inherent not at the given period but in general we can check the item that is g
spark.streams.awaitAnyTermination()
