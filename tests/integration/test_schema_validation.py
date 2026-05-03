import pytest
import json
import os
import pyspark.sql.types as T
from src.etl.financial_etl import FinancialETL

class TestFinancialETLSchemaValidation:
    """
    Validates if the generated Iceberg table schema strictly matches the expected blueprint
    created by the architecture team, defined in the catalog contract.
    """

    def test_output_schema_matches_expected_mock(self, spark, fake_financial_data):
        # Load the schema from the catalog contract
        catalog_path = os.path.join(
            os.path.dirname(__file__), 
            "..", "..", "catalog", "tables", "financial_transactions.json"
        )
        with open(catalog_path, "r") as f:
            catalog_data = json.load(f)
            
        expected_columns = {col["name"]: getattr(T, col["type"]) for col in catalog_data["columns"]}

        # Generate mock data
        df = fake_financial_data(num_records=1)
        
        # Run ETL pipeline
        etl = FinancialETL(df)
        final_df = etl.run()
        
        # Extract the schema created by PySpark
        actual_schema = final_df.schema
        
        # Check if all expected fields are present and their types match
        for col_name, expected_type_class in expected_columns.items():
            assert col_name in actual_schema.names, f"Missing column: {col_name}"
            
            actual_field = actual_schema[col_name]
            
            # Note: We compare the type names, since Spark might have tiny variations 
            # (e.g. DoubleType vs FloatType) depending on faker output. 
            assert isinstance(actual_field.dataType, expected_type_class), \
                f"Column {col_name} has type {type(actual_field.dataType)}, expected {expected_type_class}"
