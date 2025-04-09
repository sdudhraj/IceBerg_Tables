# my_dag.py
from airflow.decorators import dag
from datetime import datetime

# from pyspark import SparkContext
# from pyspark.sql import SparkSession
import pyspark
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


NESSIE_SERVER_URI = "http://nessie:19120/api/v2"
# STAGING_BUCKET = "s3://staging"
WAREHOUSE_BUCKET = "s3://warehouse"
MINIO_URI = "http://172.18.0.8:9000"


@dag(
    start_date=datetime(2025, 4, 5),
    schedule=None,
    catchup=False,
)
def my_dag():

    conf = {'spark.driver.extraJavaOptions':'-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password',
            'spark.executor.extraJavaOptions':'-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password',
            'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.103.2,software.amazon.awssdk:core:2.31.16,software.amazon.awssdk:s3:2.31.16,software.amazon.awssdk:utils:2.31.16,software.amazon.awssdk:url-connection-client:2.31.16,software.amazon.awssdk:glue:2.31.16,software.amazon.awssdk:sts:2.31.16,software.amazon.awssdk:dynamodb:2.31.16,software.amazon.awssdk:kms:2.31.16',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions',
            'spark.sql.catalog.nessie': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.nessie.uri': NESSIE_SERVER_URI,
            'spark.sql.catalog.nessie.ref': 'main',
            'spark.sql.catalog.nessie.authentication.type': 'NONE',
            'spark.sql.catalog.nessie.catalog-impl': 'org.apache.iceberg.nessie.NessieCatalog',
            "spark.sql.catalog.nessie.s3.endpoint":MINIO_URI,
            'spark.sql.catalog.nessie.warehouse': WAREHOUSE_BUCKET,
            'spark.sql.catalog.nessie.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO'}
    
    iceberg_job = SparkSubmitOperator(
        task_id="submit_job",
        conn_id="my_spark_conn",
        application="include/utils/s3_to_iceberg.py",
        verbose=True,
        conf=conf
    )
    
    iceberg_job

my_dag()