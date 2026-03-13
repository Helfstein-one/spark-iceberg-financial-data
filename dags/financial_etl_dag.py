from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add src folder to the path explicitly to import ETL classes inside the scheduler container
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'financial_glue_simulation_trigger',
    default_args=default_args,
    description='A simple DAG to trigger our mock AWS Glue PySpark job',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def _run_spark_job():
    # Inside Airflow LocalExecutor, we can invoke our PySpark classes.
    # In a real Airflow-Glue setup, this would be an `AwsGlueJobOperator`.
    from src.etl.financial_etl import FinancialETL
    from pyspark.sql import SparkSession
    from faker import Faker
    import uuid

    fake = Faker()
    # Simple faker generation here for triggering
    data = [{
        "transaction_id": str(uuid.uuid4()),
        "user_id": fake.user_name(),
        "gross_amount": round(fake.pyfloat(positive=True, min_value=10, max_value=1000), 2),
        "tax": 5.0,
        "transaction_code": "A-TEST",
        "timestamp": fake.date_time_this_year().isoformat()
    } for _ in range(100)]
    
    # Needs a dedicated connection without the testing conftest scopes
    # but reusing our OOP architecture.
    spark = SparkSession.builder.appName("airflow-triggered-job") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .master("local[*]") \
        .getOrCreate()
        
    df = spark.createDataFrame(data)
    
    etl = FinancialETL(df)
    final_df = etl.run()
    
    # Save partitioned by category on the mock S3 (MinIO)
    final_df.write \
        .format("iceberg") \
        .partitionBy("category") \
        .mode("append") \
        .save("local.financial_transactions_prod")
        
    print(f"Job finished. Ingested {final_df.count()} records.")

run_etl_task = PythonOperator(
    task_id='run_pyspark_financial_etl',
    python_callable=_run_spark_job,
    dag=dag,
)

run_etl_task
