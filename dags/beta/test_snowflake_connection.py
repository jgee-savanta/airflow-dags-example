import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import snowflake.connector
from datetime import datetime

def test_snowflake_conn_fn(**kwargs):
    # Create connection directly using snowflake.connector
    conn = snowflake.connector.connect(
        account="YNDSYIO-SAVANTAUK",
        user="AZURE_CONNECTOR",
        warehouse="WAREHOUSE_XSMALL",
        database="BRANDVUEMETA_TEST",
        role="SYSADMIN",
        private_key=os.environ.get('SNOWFLAKE_PRIVATE_KEY'),
        password=os.environ.get('SNOWFLAKE_KEY_ENCRYPTION_PASSWORD')
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION();")
    result = cursor.fetchone()
    print("Connection OK, Snowflake version:", result[0])
    cursor.close()
    conn.close()


with DAG(dag_id='beta.test_snowflake_connection', tags=['beta'], start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    test_conn = PythonOperator(
        task_id='test_conn_task',
        python_callable=test_snowflake_conn_fn
    )