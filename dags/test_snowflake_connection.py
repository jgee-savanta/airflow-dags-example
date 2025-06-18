import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.snowflake_utils import get_snowflake_connection
from datetime import datetime

def test_snowflake_conn_fn(**kwargs):
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION();")
        result = cursor.fetchone()
        print("Connection OK, Snowflake version:", result[0])


with DAG(dag_id='test_snowflake_connection', tags=[os.environ.get('ENVIRONMENT')], start_date=datetime(2025, 1, 1), schedule=None, catchup=False) as dag:
    test_conn = PythonOperator(
        task_id='test_conn_task',
        python_callable=test_snowflake_conn_fn
    )