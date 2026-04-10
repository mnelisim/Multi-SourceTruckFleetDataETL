import requests
import os
from config import webhook_url

# spark transform alerts
def alert_on_empty_batch(batch_id):
    """Simple alert for Spark Streaming DQ issues"""
    webhook_url = os.getenv("SLACK_WEBHOOK")
    message = f"*Data Quality Alert (Spark)*\nBatch {batch_id} contained 0 valid records after validation filters."
    
    try:
        requests.post(webhook_url, json={"text": message})
    except Exception as e:
        print(f"Failed to send Spark alert: {e}")
        
# Airflow alerts for task
def alert_on_failure(context):

    message = f"""
    Task Failed!
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution Time: {context['execution_date']}
    Log URL: {context['task_instance'].log_url}
    """

    try:
        requests.post(webhook_url, json={"text": message})
    except Exception as e:
        print(f"Failed to send alert: {e}")
