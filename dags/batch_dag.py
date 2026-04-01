from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

email = 'saspizzox@gmail.com'

default_args = {
    'owner': 'Pizz0x',
    'email': [email],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'Retail_Data_Pipeline',
    default_args = default_args,
    start_date = datetime(2026,3,30),
    schedule_interval = '0 2 * * *',
) as dag:
    
    # wait that the folder of today exists on MinIO
    sensor = S3KeySensor(
        task_id='sensor',
        bucket_name='retail.datalake',
        bucket_key='silver/receipts/year={{ execution_date.year }}/month={{ execution_date.month }}/day={{ execution_date.day }}/*',
        aws_conn_id='minio_s3_conn', # connection to minio on airflow
        poke_interval=60 * 10, # check every 10 minutes
        timeout=60 * 60 * 2,  # forfeit after 2 hours
        mode='poke'
    )

    # send a mail to the user in case of success (only if Spark works fine, indeed thanks to the configuration in case of failure airflow already send an email)
    success_mail = EmailOperator(
        task_id='success_mail',
        to=email,
        subject='[SUCCESS] Batch Aggregation',
        html_content = '<h3>Good News !!</h3><p>Data of day {{ ds }} have been succesfully aggregated on Clickhouse and are reasy to be used.</p>'

    )

    batch_aggregations = SparkSubmitOperator(
        task_id = 'batch_aggregations',
        application = '../batch_processor.py',
        conn_id = 'spark_default',
        application_args = ['--date', '{{ds}}'],
        conf = {'spark.master': 'local[*]'}
    )
    sensor >> batch_aggregations >> success_mail