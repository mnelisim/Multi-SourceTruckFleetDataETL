import requests
from config import webhook_url

#Airflow alerts for task
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