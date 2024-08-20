from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pymssql

# Define a function to test MSSQL connection
def test_fs1_connection():
    conn_id = 'fs1_rmo_ice'  # Replace with your connection ID
    #share_name = 'ICERMO_Tech'
    directory = '/rmo_ct_prod/'
    path = directory


    # Construct the connection parameters
    host = conn.host
    database = conn.schema
    user = conn.login
    password = conn.password

    hook = SambaHook(conn_id)
    files = hook.listdir(path)
    for f in files:
        print(f)


# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'test_fs1_connection',
    default_args=default_args,
    description='A simple DAG for testin FS1 connection',
    schedule_interval=None,  # Set to None to run manually or specify a cron schedule
    start_date=days_ago(1),
    catchup=False,
)

# Define the task
test_connection_task = PythonOperator(
    task_id='test_fs1_connection_task',
    python_callable=test_fs1_connection,
    dag=dag,
)

# Set task dependencies
test_connection_task