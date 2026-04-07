from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Add project root to path
sys.path.append('/opt/airflow')

class TruckIngestManager:
    """Manages the Ingestion lifecycle for the Truck Fleet."""

    def __init__(self):
        self.dag_id = 'truck_fleet_ingest_mission'
        self.default_args = {
            'owner': 'mnelisi',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

    @staticmethod
    def initialize_db():
        """Logic to setup Postgres tables."""
        from storage.database_setup import DatabaseSetup
        db_setup = DatabaseSetup()
        db_setup.initialize_database()

    @staticmethod
    def run_producer():
        """Logic to trigger the Kafka Producer."""
        from ingestion.data_fetching import Producer
        producer = Producer()
        producer.run_fleet()

    def create_dag(self):
        """Creates and returns the Airflow DAG object."""
        with DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            start_date=datetime(2024, 3, 28),
            schedule_interval=None,
            catchup=False,
            tags=['ingestion', 'producer']
        ) as dag:

            setup_task = PythonOperator(
                task_id='setup_postgres_table',
                python_callable=self.initialize_db
            )

            ingest_task = PythonOperator(
                task_id='ingest_to_kafka',
                python_callable=self.run_producer
            )

            setup_task >> ingest_task
            
            return dag

# Instantiate and expose the DAG to Airflow
manager = TruckIngestManager()
dag = manager.create_dag()
