import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def test_snowflake_conn_fn(**kwargs):
    private_key = f"-----BEGIN ENCRYPTED PRIVATE KEY-----\n{os.environ.get('SNOWFLAKE_PRIVATE_KEY')}\n-----END ENCRYPTED PRIVATE KEY-----"
    private_key_passphrase = os.environ.get('SNOWFLAKE_KEY_ENCRYPTION_PASSWORD')
    
    # Create connection parameters
    conn_params = {
        "account": "YNDSYIO-SAVANTAUK",
        "user": "AZURE_CONNECTOR",
        "warehouse": "WAREHOUSE_XSMALL",
        "database": "BRANDVUEMETA_TEST",
        "role": "SYSADMIN",
        "private_key": private_key,
        "private_key_passphrase": private_key_passphrase
    }

    hook = SnowflakeHook(snowflake_conn_id=None, **conn_params)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION();")   # any lightweight query
    result = cursor.fetchone()
    print("Connection OK, Snowflake version:", result[0])
    cursor.close()
    conn.close()


with DAG(dag_id='beta.test_snowflake_connection', tags=['beta'], start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    test_conn = PythonOperator(
        task_id='test_conn_task',
        python_callable=test_snowflake_conn_fn
    )