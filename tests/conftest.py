import os
import pytest
from pyspark.sql import SparkSession
from faker import Faker
import uuid

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell"

fake = Faker()

@pytest.fixture(scope="session")
def spark():
    """
    Creates a local SparkSession for testing, ignoring S3/Iceberg configs
    for pure unit testing speed, or mocking them as needed.
    """
    iceberg_version = "3.5_2.12:1.5.0"
    
    spark = SparkSession.builder \
        .appName("pytest-spark-testing") \
        .master("local[2]") \
        .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-{iceberg_version},org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "file:///tmp/warehouse") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="function")
def fake_financial_data(spark):
    """
    Generates a DataFrame with fake financial transactions 
    using Faker.
    """
    def _generate_data(num_records=10):
        data = []
        for _ in range(num_records):
            
            # Gross amount between 10 and 1000
            gross_amount = round(fake.pyfloat(positive=True, min_value=10, max_value=1000), 2)
            
            # Tax is 10% or some random amount less than gross
            tax = round(gross_amount * 0.1, 2)
            
            # Codes A, B, or C
            code = fake.random_element(elements=("A-123", "B-456", "C-789"))
            
            record = {
                "transaction_id": str(uuid.uuid4()),
                "user_id": fake.user_name(),
                "gross_amount": gross_amount,
                "tax": tax,
                "transaction_code": code,
                "timestamp": fake.date_time_this_year().isoformat()
            }
            data.append(record)
            
        return spark.createDataFrame(data)
    
    return _generate_data
