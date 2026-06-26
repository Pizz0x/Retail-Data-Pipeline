from dotenv import find_dotenv, load_dotenv
import os, pyspark 
from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    load_dotenv(find_dotenv())
    s3_user = os.environ.get('S3_USER', 'user')
    s3_pass = os.environ.get('S3_PASSWORD', 'password')

    maven_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.10.0"
    ]

    return SparkSession.Builder() \
    .appName(app_name) \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.scheduler.mode", "FAIR") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.memory.offHeap.size", "512m") \
    .config("spark.hadoop.fs.s3a.access.key", s3_user) \
    .config("spark.hadoop.fs.s3a.secret.key", s3_pass) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config('spark.jars.packages', ','.join(maven_packages)) \
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