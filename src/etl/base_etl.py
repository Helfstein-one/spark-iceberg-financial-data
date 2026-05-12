import os

# Ensure packages are downloaded by Spark before JVM starts via Python execution
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.iceberg:iceberg-aws-bundle:1.5.0 pyspark-shell"

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
        warehouse_path = os.getenv("WAREHOUSE_PATH", "s3://warehouse/")
        
        # Iceberg packages
        iceberg_version = "3.5_2.12:1.5.0"
        nessie_version = "0.77.1"
        aws_bundle_version = "2.20.18"
        nessie_uri = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")

        builder = SparkSession.builder \
            .appName(self.job_name) \
            .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-{iceberg_version},org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:{nessie_version},org.apache.iceberg:iceberg-aws-bundle:{iceberg_version.split(':')[1]}") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
            .config("spark.sql.catalog.nessie.uri", nessie_uri) \
            .config("spark.sql.catalog.nessie.ref", "main") \
            .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
            .config("spark.sql.catalog.nessie.warehouse", warehouse_path) \
            .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.nessie.s3.endpoint", s3_endpoint) \
            .config("spark.sql.catalog.nessie.s3.access-key-id", s3_access_key) \
            .config("spark.sql.catalog.nessie.s3.secret-access-key", s3_secret_key) \
            .config("spark.sql.catalog.nessie.s3.path-style-access", "true") \
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
            .saveAsTable(f"nessie.{table_name}")

    def run(self):
        """Main execution flow"""
        import logging
        import time
        logger = logging.getLogger(self.job_name)
        
        start_time = time.time()
        logger.info(f"Starting job: {self.job_name}")
        
        extracted_df = self.extract()
        extracted_count = extracted_df.count()
        logger.info(f"Extracted {extracted_count} rows")
        
        transformed_df = self.transform(extracted_df)
        transformed_count = transformed_df.count()
        logger.info(f"Transformed {transformed_count} rows")
        
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Job {self.job_name} finished in {execution_time:.2f} seconds")
        
        return transformed_df
