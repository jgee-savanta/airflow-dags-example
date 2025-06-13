import os
import tempfile
from airflow import DAG
from airflow.operators.python import PythonOperator
import snowflake.connector
from datetime import datetime

def test_snowflake_conn_fn(**kwargs):
    private_key_pem = f"-----BEGIN ENCRYPTED PRIVATE KEY-----\n{os.environ.get('SNOWFLAKE_PRIVATE_KEY')}\n-----END ENCRYPTED PRIVATE KEY-----"

    with tempfile.NamedTemporaryFile(delete=False, mode='w') as key_file:
        key_file_path = key_file.name
        key_file.write(private_key_pem)

    conn = snowflake.connector.connect(
        account="YNDSYIO-SAVANTAUK",
        user="AZURE_CONNECTOR",
        warehouse="WAREHOUSE_XSMALL",
        database="BRANDVUEMETA_TEST",
        role="SYSADMIN",
        private_key_file=key_file_path,
        private_key_file_pwd=os.environ.get('SNOWFLAKE_KEY_ENCRYPTION_PASSWORD')
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION();")
    result = cursor.fetchone()
    print("Connection OK, Snowflake version:", result[0])
    cursor.close()
    conn.close()


with DAG(dag_id='test.test_snowflake_connection', tags=['beta'], start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    test_conn = PythonOperator(
        task_id='test_conn_task',
        python_callable=test_snowflake_conn_fn
    )