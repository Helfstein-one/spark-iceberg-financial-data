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
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.iceberg:iceberg-aws-bundle:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")) \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
        .config("spark.sql.catalog.nessie.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.nessie.s3.access-key-id", "minioadmin") \
        .config("spark.sql.catalog.nessie.s3.secret-access-key", "minioadmin") \
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true") \
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
        .saveAsTable("nessie.financial_transactions_prod")
        
    print(f"Job finished. Ingested {final_df.count()} records.")

if __name__ == "__main__":
    main()
