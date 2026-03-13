import os
from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class BaseETL(ABC):
    """
    Base class for PySpark ETL jobs using Apache Iceberg and MinIO/S3.
    """
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.spark = self._initialize_spark()

    def _initialize_spark(self) -> SparkSession:
        """
        Initializes SparkSession with Iceberg and AWS S3/MinIO configurations.
        """
        # Read from environment or use MinIO defaults for local dev
        s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        warehouse_path = os.getenv("WAREHOUSE_PATH", "s3a://warehouse/")
        
        # Iceberg packages
        iceberg_version = "3.5_2.12:1.5.0"
        aws_bundle_version = "2.20.18"

        builder = SparkSession.builder \
            .appName(self.job_name) \
            .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-{iceberg_version},org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", warehouse_path) \
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        return builder.getOrCreate()

    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source."""
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform data."""
        pass

    def load_to_iceberg(self, df: DataFrame, table_name: str, mode: str = "append"):
        """
        Load dataframe to an Iceberg table.
        """
        df.write \
            .format("iceberg") \
            .mode(mode) \
            .save(f"local.{table_name}")

    def run(self):
        """Main execution flow"""
        extracted_df = self.extract()
        transformed_df = self.transform(extracted_df)
        return transformed_df
