import pytest
from src.etl.financial_etl import FinancialETL

class TestFinancialETLUnit:
    """
    Unit tests for the specific transformation methods.
    We test each function in isolation.
    """

    def test_calculate_net_amount(self, spark, fake_financial_data):
        df = fake_financial_data(num_records=5)
        etl = FinancialETL(df)
        
        # Test specific method
        result_df = etl._calculate_net_amount(df)
        
        # Collect and verify
        rows = result_df.collect()
        for row in rows:
            expected_net = round(row["gross_amount"] - row["tax"], 2)
            assert round(row["net_amount"], 2) == expected_net

    def test_categorize_transaction(self, spark, fake_financial_data):
        # We manually craft a small df to test specific branches of `when`
        data = [
            {"transaction_code": "A-001", "id": 1},
            {"transaction_code": "B-002", "id": 2},
            {"transaction_code": "X-003", "id": 3}
        ]
        df = spark.createDataFrame(data)
        etl = FinancialETL(df)
        
        result_df = etl._categorize_transaction(df)
        categories = {row["id"]: row["category"] for row in result_df.collect()}
        
        assert categories[1] == "credit"
        assert categories[2] == "debit"
        assert categories[3] == "other"

    def test_anonymize_user_id(self, spark, fake_financial_data):
        data = [{"user_id": "mauricio_admin"}]
        df = spark.createDataFrame(data)
        etl = FinancialETL(df)
        
        result_df = etl._anonymize_user_id(df)
        row = result_df.collect()[0]
        
        anon_user = row["anonymized_user"]
        
        assert anon_user.startswith("ma***")
        assert len(anon_user) == 13 # 'ma' (2) + '***' (3) + hash (8)
