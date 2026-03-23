from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckSilver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
# Leggi l'intera cartella Silver
df_silver = spark.read.parquet("s3a://retail.datalake/silver/receipts/")

# Controlli da fare:
print(f"Totale righe salvate: {df_silver.count()}")
df_silver.show(10)

# Verifica se ci sono duplicati (se dropDuplicates ha funzionato)
duplicati = df_silver.groupBy("receipt_id", "timestamp").count().filter("count > 1")
print(f"Trovati {duplicati.count()} duplicati!")