import pytest
import time
from src.etl.financial_etl import FinancialETL

class TestFinancialETLPerformance:
    """
    Performance test to ensure the transformations can handle larger datasets.
    """

    # Skip in typical CI pipelines if needed or tag it properly, but good for local/nightly runs
    @pytest.mark.performance
    def test_performance_large_volume(self, spark, fake_financial_data):
        # Generate 100k rows
        # For pure unit testing performance, we can just parallelize a large dataframe locally
        num_records = 100000
        
        start_time_gen = time.time()
        print(f"Generating {num_records} fake records... this might take a moment")
        df = fake_financial_data(num_records=num_records)
        print(f"Generation took {time.time() - start_time_gen:.2f} seconds")

        etl = FinancialETL(df)
        
        start_time_etl = time.time()
        final_df = etl.run()
        
        # Action to force execution
        count = final_df.count()
        duration = time.time() - start_time_etl
        
        print(f"ETL of {count} records took {duration:.2f} seconds")
        
        # Assert that it completes within an acceptable local threshold (e.g., 20 seconds)
        assert duration < 20.0
        assert count == num_records
