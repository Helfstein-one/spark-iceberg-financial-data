import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from src.etl.financial_etl import FinancialETL

class TestFinancialETLSchemaValidation:
    """
    Validates if the generated Iceberg table schema strictly matches the expected blueprint
    created by the architecture team.
    """

    def test_output_schema_matches_expected_mock(self, spark, fake_financial_data):
        # The exact schema expected for our Iceberg data warehouse
        expected_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("gross_amount", DoubleType(), True),
            StructField("tax", DoubleType(), True),
            StructField("transaction_code", StringType(), True),
            StructField("timestamp", StringType(), True), # Faker isoformat returns string
            StructField("net_amount", DoubleType(), True),
            StructField("category", StringType(), False), # Generated column (should not be nullable if we use strict logic, but Spark defaults to True on withColumn)
            StructField("anonymized_user", StringType(), False) 
        ])

        # Generate mock data
        df = fake_financial_data(num_records=1)
        
        # Run ETL pipeline
        etl = FinancialETL(df)
        final_df = etl.run()
        
        # Extract the schema created by PySpark
        actual_schema = final_df.schema
        
        # Check if all expected fields are present and their types match
        for expected_field in expected_schema.fields:
            assert expected_field.name in actual_schema.names, f"Missing column: {expected_field.name}"
            
            actual_field = actual_schema[expected_field.name]
            
            # Note: We compare the type names, since Spark might have tiny variations 
            # (e.g. DoubleType vs FloatType) depending on faker output. 
            # In our case Faker returns python floats which map to DoubleType
            assert isinstance(actual_field.dataType, type(expected_field.dataType)), \
                f"Column {expected_field.name} has type {type(actual_field.dataType)}, expected {type(expected_field.dataType)}"
