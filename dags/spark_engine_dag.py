from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.alerts import alert_on_failure
import sys
import os

# Add project root to path so Airflow can find modules
sys.path.append('/opt/airflow')

class SparkStreamingManager:
    def __init__(self):
        self.dag_id = 'spark_streaming_engine'
        self.default_args = {
            'owner': 'mnelisi',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            "on_failure_callback": alert_on_failure
        }
        # Centralized command config
        self.script_path = "/opt/spark_jobs/transform.py"
        self.checkpoint_path = "/opt/spark_jobs/checkpoints/truck_fleet"

    def get_bash_command(self):
        """Constructs the idempotent spark-submit command."""
        return (
            f"docker exec -u root spark pip install python-dotenv && " 
            f"docker exec -u root spark pkill -9 -f {os.path.basename(self.script_path)} || true && "
            "sleep 5 && "
            f"docker exec -u root spark rm -rf {self.checkpoint_path}/offsets && "
            "docker exec -u root spark /opt/spark/bin/spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2 "
            f"{self.script_path}"
        )

    def create_dag(self):
        """Generates the DAG object."""
        with DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            start_date=datetime(2024, 3, 28),
            schedule_interval=None,
            catchup=False
        ) as dag:
            
            run_spark = BashOperator(
                task_id="run_spark_job",
                bash_command=self.get_bash_command()
            )
            run_spark
            return dag

# Instantiate the class and create the DAG for Airflow to detect
manager = SparkStreamingManager()
dag = manager.create_dag()
