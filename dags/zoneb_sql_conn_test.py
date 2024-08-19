from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pymssql

# Define a function to test MSSQL connection
def test_mssql_connection():
    conn_id = 'mssql_default'  # Replace with your connection ID
    conn = BaseHook.get_connection(conn_id)

    # Construct the connection parameters
    host = conn.host
    database = conn.schema
    user = conn.login
    password = conn.password

    try:
        with pymssql.connect(host=host, database=database, user=user, password=password) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES")

            # Fetch result
            row = cursor.fetchone()
            print('Number of tables:', row[0])

            # Close connection
            conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

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
    'test_mssql_connection',
    default_args=default_args,
    description='A simple DAG to test MSSQL connection using pymssql',
    schedule_interval=None,  # Set to None to run manually or specify a cron schedule
    start_date=days_ago(1),
    catchup=False,
)

# Define the task
test_connection_task = PythonOperator(
    task_id='test_mssql_connection_task',
    python_callable=test_mssql_connection,
    dag=dag,
)

# Set task dependencies
test_connection_task