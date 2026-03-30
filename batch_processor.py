import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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