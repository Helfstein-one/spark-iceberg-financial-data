from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

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

# Use SparkSubmitOperator to decouple execution
run_etl_task = SparkSubmitOperator(
    task_id='run_pyspark_financial_etl',
    application='/opt/airflow/src/scripts/run_financial_etl.py',
    conn_id='spark_default',
    # Pass relevant packages here or inside the python script's spark session
    packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
    name='airflow-triggered-job',
    verbose=True,
    dag=dag,
)

run_etl_task
