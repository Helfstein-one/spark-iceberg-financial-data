# Project Backlog & Improvements

This document outlines potential improvements and technical debt that can be addressed in future iterations of the project.

## 1. Airflow Orchestration & Scaling
* **Spark Operator**: Instead of using `PythonOperator` to trigger Spark directly, we should migrate to an Airflow `SparkSubmitOperator`, `DockerOperator`, or `KubernetesPodOperator`. This will allow Spark workloads to scale independently from the Airflow scheduler and decouple execution.

## 2. Code Architecture & Observability
* **Job Metrics Logging**: Add a standardized logging mechanism inside `BaseETL` to push job metrics (like rows processed, execution time) to a monitoring system or simply to structured Airflow task logs.

## 3. Testing Coverage
* **Edge Cases**: Ensure there are comprehensive unit tests for edge cases in data transformations. For instance, testing `FinancialETL._calculate_net_amount` with nulls, zero, or negative numbers.
