from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def test_snowflake_conn_fn(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id='snowflake')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION();")   # any lightweight query
    result = cursor.fetchone()
    print("Connection OK, Snowflake version:", result[0])
    cursor.close()
    conn.close()


with DAG(dag_id='beta.test_snowflake_connection', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    test_conn = PythonOperator(
        task_id='test_conn_task',
        python_callable=test_snowflake_conn_fn
    )