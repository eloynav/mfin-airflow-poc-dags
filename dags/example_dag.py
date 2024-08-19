from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kubernetes_pod_operator_example',
    default_args=default_args,
    description='A simple DAG to demonstrate KubernetesPodOperator',
    schedule_interval=timedelta(days=1),
)

# Define task
task1 = KubernetesPodOperator(
    task_id="kubernetes_pod_operator_task",
    namespace='default',
    service_account_name="airflow-admin",
    image="python:3.8",
    cmds=["python", "-c"],
    arguments=["print('Hello, Kubernetes!')"],
    labels={"Release": "airflow"},
    name="kubernetes_pod_operator_task",
    get_logs=True,
    dag=dag,
)

# Define dependencies
task1