from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, FloatType

from .base_etl import BaseETL

class FinancialETL(BaseETL):
    """
    Concrete ETL class for processing financial transactions.
    """
    
    def __init__(self, raw_df: DataFrame):
        super().__init__(job_name="financial_transactions_etl")
        self.raw_df = raw_df

    def extract(self) -> DataFrame:
        """
        Returns the provided raw_df. In a real scenario, this would read from a source.
        For our testing the Faker fixtures will generate this DataFrame.
        """
        return self.raw_df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Orchestrates all specific column transformations.
        """
        df = self._calculate_net_amount(df)
        df = self._categorize_transaction(df)
        df = self._anonymize_user_id(df)
        return df

    def _calculate_net_amount(self, df: DataFrame) -> DataFrame:
        """
        Creates 'net_amount' by subtracting 'tax' from 'gross_amount'.
        """
        return df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("tax")
        )

    def _categorize_transaction(self, df: DataFrame) -> DataFrame:
        """
        Creates 'category' based on the 'transaction_code'.
        """
        # Example business logic: codes starting with 'A' are 'credit', 'B' are 'debit', else 'other'
        return df.withColumn(
            "category",
            F.when(F.col("transaction_code").startswith("A"), "credit")
             .when(F.col("transaction_code").startswith("B"), "debit")
             .otherwise("other")
        )

    def _anonymize_user_id(self, df: DataFrame) -> DataFrame:
        """
        Creates an 'anonymized_user' masking the original 'user_id' for PII protection.
        """
        # Example: Keeps first 2 characters, hashes the rest using sha2
        return df.withColumn(
            "anonymized_user",
            F.concat(
                F.substring(F.col("user_id"), 1, 2),
                F.lit("***"),
                F.substring(F.sha2(F.col("user_id").cast(StringType()), 256), 1, 8)
            )
        )
