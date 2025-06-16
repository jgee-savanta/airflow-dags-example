import os
import tempfile
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime


def copy_sqlserver_to_snowflake(**context):
    mssql_conn_id = 'azure_sql_vm'
    
    source_table = 'dbo.Features'
    target_table = 'BrandVueMeta_Test.vue.Features'.upper()
    
    # Step 1: Extract from SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    df = mssql_hook.get_pandas_df(f"SELECT * FROM {source_table}")
    df.rename(columns=lambda x: x.upper(), inplace=True)  # Ensure column names are uppercase to match Snowflake
    
    # Step 2: Truncate Snowflake table
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
    truncate_sql = f"TRUNCATE TABLE {target_table}"
    cursor.execute(truncate_sql)

    # Step 3: Load data into Snowflake
    success = write_pandas(
        conn=conn,
        df=df,
        table_name=target_table.split('.')[-1],
        schema=target_table.split('.')[-2],
        database=target_table.split('.')[-3],
        quote_identifiers=True,
        # auto_create_table=True, 
    )
    assert success, f"Data load to Snowflake failed for table {target_table}"

with DAG(dag_id='live.fullcopy_sqlserver_to_snowflake', tags=['live'], start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    copy_task = PythonOperator(
        task_id='fullcopy_sqlserver_to_snowflake_task',
        python_callable=copy_sqlserver_to_snowflake
    )

    copy_task