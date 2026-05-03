import sys
import os
from faker import Faker
import uuid

# Add src folder to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.etl.financial_etl import FinancialETL
from pyspark.sql import SparkSession

def main():
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

if __name__ == "__main__":
    main()
