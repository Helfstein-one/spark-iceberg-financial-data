import pytest
from src.etl.financial_etl import FinancialETL

class TestFinancialETLIntegration:
    """
    Integration tests to ensure the full pipeline runs from end-to-end
    generating data and attempting a local Iceberg write.
    """

    def test_full_pipeline_process(self, spark, fake_financial_data, monkeypatch):
        monkeypatch.setattr(FinancialETL, "_initialize_spark", lambda self: spark)

        # 1. Generate fake source data
        df = fake_financial_data(num_records=50)
        
        # 2. Instantiate OOP ETL
        etl = FinancialETL(df)
        
        # 3. Process end-to-end
        final_df = etl.run()
        
        # 4. Assertions on the result schema and content
        columns = final_df.columns
        assert "net_amount" in columns
        assert "category" in columns
        assert "anonymized_user" in columns
        
        # Assertions on data logical flow
        # Ensure all records were processed
        assert final_df.count() == 50
        
        # 5. Local write test (mocking Iceberg save in local warehouse directory)
        etl.load_to_iceberg(final_df, "financial_transactions", mode="overwrite")
        
        # 6. Read back from local catalog to assure persistence worked
        read_df = spark.table("nessie.financial_transactions")
        assert read_df.count() == 50
