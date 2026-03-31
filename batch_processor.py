import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True)
    args = parser.parse_args()

    execution_date = args.date
    print(f"Elaboration of the batch for date: {execution_date}")

    y,m,d = execution_date.split('-')
    m = str(int(m)) 
    d = str(int(d))

    spark = SparkSession.builder \
        .appName(f"Batch_Processor_{execution_date}") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.10.0,com.clickhouse:clickhouse-jdbc:0.9.5") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    silver_path = f"s3a://retail.datalake/silver/receipts/year={y}/month={m}/day={d}/"
    print(f"The silver data are contained in: {silver_path}")

    try:
        silver_data = spark.read.parquet(silver_path)
    except Exception as e:
        print(f"There was an Error / No data found for {silver_path}: {e}")
        spark.stop()
        return
    
    daily_data = silver_data \
        .groupBy(
            col("category"),
            col("model"),
            col("sex"),
            col("supplier"),
            col("store"),
            col("region"),
            col("loc_type"),
            col("square_footage"),
        ) \
        .agg(
            sum(when(col("transaction_type")=="SALE", col("quantity")).otherwise(0)).alias("sold_articles"),
            sum(col("net_profit")).alias("net_profit"),  # profit is already computed on the quantity of articles sold and is negative in case of return
            sum(when(col("transaction_type")=="RETURN", col("quantity")).otherwise(0)).alias("returned_articles"),
            sum(when(col("transaction_type")=="SALE",-col("cost")*col("quantity"))
                .when(col("transaction_type")=="RETURN", col("cost")*col("quantity"))
                .otherwise(0)).alias("costs"),
            sum(when(col("transaction_type")=="SALE",col("list_price")*col("quantity"))
                .when(col("transaction_type")=="RETURN", -col("list_price")*col("quantity"))
                .otherwise(0)).alias("theoretic_profit"),
            
        ) \
        .withColumn(
            "return_rate",
            (col("returned_articles") / (col("sold_articles")+col("returned_articles")))*100
        ) \
        .withColumn(
            "net_margin",
            col("net_profit") / col("theoretic_profit") * 100
        ) \
        .withColumn(
            "date",
            lit(execution_date)
        ) \
        .drop("theoretic_profit") \
        .fillna(0, subset=["return_rate", "net_margin"])

    daily_data.show()


    daily_data.write \
        .mode("append") \
        .format("clickhouse") \
        .option("host", "clickhouse") \
        .option("port", "8123") \
        .option("database", "retail_stats") \
        .option("table", "daily_data") \
        .option("user", "default") \
        .option("password", "clickhouse123") \
        .option("batchsize", "5000") \
        .save()
    
    print("Batch Pipeline completed !!")
    spark.stop()

    
if __name__ == "__main__":
    main()
