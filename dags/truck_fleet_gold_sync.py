from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


class GoldSyncManager:
    def __init__(self):
        self.dag_id = "truck_fleet_gold_sync"
        self.default_args = {
            'owner': 'mnelisi',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
    @staticmethod
    def run_gold_load():
        # This calls existing ETL logic to move Postgres -> Parquet -> BigQuery
        from pipeline.etl_pipeline import ETLpipeline
        pipeline = ETLpipeline()
        pipeline.run_batch_load()

    def create_dag(self):
        with DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            start_date=datetime(2024, 3, 28),
            schedule_interval='@hourly', # Runs every hour to sync live data to the cloud
            catchup=False
        ) as dag:

         sync_to_bigquery = PythonOperator(
            task_id='postgres_to_bigquery',
            python_callable=self.run_gold_load
        )

        sync_to_bigquery
        return dag

# Instantiate the class and create the DAG for Airflow to detect
manager = GoldSyncManager()
dag = manager.create_dag()

