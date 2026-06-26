import pyspark
from pyspark.sql.functions import from_json, col, when, sum, count, window
import os
from sparksession import create_spark_session
from silver_sink import silver_schema
from streaming_processor import receipt_schema
from dotenv import load_dotenv, find_dotenv

# search for the .env file and load the variables in the script
load_dotenv(find_dotenv())
ch_user = os.environ.get("CH_USER", "user")
ch_pass = os.environ.get("CH_PASSWORD", "password")

# create the spark session and configure the kafka connector
spark_version = pyspark.__version__

spark = create_spark_session(f"Gold-Sink")
    

# hidden warnings (they are lame)
spark.sparkContext.setLogLevel("WARN")

### DEFINITION OF DATA SCHEMA -> we need to specify it cause Spark is not able to infer the correct types in case of continuos streaming of data


### READ FROM KAFKA PIPELINE
kafka_receipts_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "receipts_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 10000) \
    .load()
#startingOffsets =  latest  -> required for streaming data, otherwise we use earliest for batch. It tells us to read only new messages, ignoring the previous ones

receipt_data = kafka_receipts_data \
    .select(from_json(col("value").cast("string"), receipt_schema).alias("data")) \
    .select("data.*") \


kafka_engineered_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "silver_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 10000) \
    .load()
#startingOffsets =  latest  -> required for streaming data, otherwise we use earliest for batch. It tells us to read only new messages, ignoring the previous ones

engineered_data = kafka_engineered_data \
    .select(from_json(col("value").cast("string"), silver_schema).alias("data")) \
    .select("data.*") \


### STATEFUL AGGREGATIONS

# check number of receipt for each type of payment in a given checkout / store (so we use receipt_data and not items_data), used to detect problem of a checkout of internet connection in a store
payment_stats = receipt_data \
    .groupBy(
        window(col("timestamp"), "1 minutes"),
        col("store"),
        col("checkout"),
        col("payment")
    ) \
    .agg(count("*").alias("receipt_number"))

payment_stats = payment_stats.select(
    col("store"),
    col("checkout"),
    col("payment"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("receipt_number")
)

# check the article that is being more sold and the profit that it gives in a store at the moment, at the same time check the return rate on the articles (if too high it means that the product has some kind of difects)
article_stats = engineered_data \
    .groupBy(
        window(col("timestamp"), "1 minutes", "30 seconds"),
        col("category"),
        col("model"),
        col("sex"),
        col("supplier"),
        col("store")
    ) \
    .agg(
        sum(when(col("transaction_type")=="SALE", col("quantity")).otherwise(0)).alias("sold_articles"),
        sum(col("net_profit")).alias("net_profit_articles"),  # profit is already computed on the quantity of articles sold and is negative in case of return
        sum(when(col("transaction_type")=="RETURN", col("quantity")).otherwise(0)).alias("returned_articles"),
    ) \
    .withColumn(
        "return_rate",
        (col("returned_articles") / (col("sold_articles")+col("returned_articles")))*100
    )

article_stats = article_stats.select(
    col("category"),
    col("model"),
    col("sex"),
    col("store"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("supplier"),
    col("sold_articles"),
    col("net_profit_articles"),
    col("returned_articles"),
    col("return_rate")
)

# check the checkout and so even the store which is getting more profit and revenue at the moment, at the same moment we check the return rate (so that the manager can know if a cashier is a dodger)
# we also check the payment methods (in this way we can notice if there could be some problem with card payments and other things)
store_checkout_stats = engineered_data \
    .groupBy(
        window(col("timestamp"), "1 minutes", "30 seconds"),
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
        sum(when(col("transaction_type")=="SALE",-col("cost")*col("quantity"))
            .when(col("transaction_type")=="RETURN", col("cost")*col("quantity"))
            .otherwise(0)).alias("ck_costs"),
        sum(when(col("transaction_type") == "SALE", col("quantity")).otherwise(0)).alias("ck_total_sales"),
        sum(when(col("transaction_type") == "RETURN", col("quantity")).otherwise(0)).alias("ck_total_return"),
        sum(when(col("transaction_type") == "SALE", col("discount")).otherwise(0)).alias("total_discount") # this has then to be divided by the quantity on the interface platform that does the graphics (if we do the average directly here for the checkout then it would not be possible to do so even for the stores)
    ) \
    .withColumn(
        "ck_return_rate",
        (col("ck_total_return") / (col("ck_total_return")+col("ck_total_sales")))*100
    ) \
    .withColumn(
        "ck_net_margin",
        (col("ck_net_profit")/ col("ck_theoretic_profit")) * 100
    ) \
    .withColumn(
        "ck_discount",
        col("total_discount") / col("ck_total_sales")
    ) \
    .drop("total_discount", "ck_theoretic_profit") \
    .fillna(0, subset=["ck_return_rate", "ck_net_margin", "ck_discount"])

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
    col("ck_costs"),
    col("ck_total_sales"),
    col("ck_total_return"),
    col("ck_discount"),
    col("ck_return_rate"),
    col("ck_net_margin")
)

# function to write batch in the databases
def ch_payment(df_batch, epoch_id):
    df_batch.write \
        .format("clickhouse") \
        .option("host", "clickhouse-gold") \
        .option("port", "8123") \
        .option("user", ch_user) \
        .option("password", ch_pass) \
        .option("database", "retail_stats") \
        .option("table", "payment_analytics") \
        .option("batchSize", "5000") \
        .mode("append") \
        .save()
    
payment_query = payment_stats.writeStream \
    .outputMode("append") \
    .foreachBatch(ch_payment) \
    .option("checkpointLocation", "./checkpoints/gold/payments") \
    .trigger(processingTime="15 seconds") \
    .start()

def ch_article(df_batch, epoch_id):
    df_batch.write \
        .format("clickhouse") \
        .option("host", "clickhouse-gold") \
        .option("port", "8123") \
        .option("user", ch_user) \
        .option("password", ch_pass) \
        .option("database", "retail_stats") \
        .option("table", "article_analytics") \
        .option("batchSize", "5000") \
        .mode("append") \
        .save()

article_store_query = article_stats.writeStream \
    .outputMode("append") \
    .foreachBatch(ch_article) \
    .option("checkpointLocation", "./checkpoints/gold/articles/") \
    .trigger(processingTime="15 seconds") \
    .start()

def ch_checkout(df_batch, epoch_id):
    df_batch.write \
        .format("clickhouse") \
        .option("host", "clickhouse-gold") \
        .option("port", "8123") \
        .option("user", ch_user) \
        .option("password", ch_pass) \
        .option("database", "retail_stats") \
        .option("table", "checkout_analytics") \
        .option("batchSize", "5000") \
        .mode("append") \
        .save()

store_checkout_query = store_checkout_stats.writeStream \
    .outputMode("append") \
    .foreachBatch(ch_checkout) \
    .option("checkpointLocation", "./checkpoints/gold/checkouts/") \
    .trigger(processingTime="15 seconds") \
    .start()

spark.streams.awaitAnyTermination()