import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.snowflake_utils import get_snowflake_connection


def copy_sqlserver_to_snowflake(**context):
    mssql_conn_id = 'azure_sql_vm'
    source_table = 'dbo.Features'
    target_table = 'BrandVueMeta_Test.vue.Features'.upper()
    
    # Step 1: Extract from SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    df = mssql_hook.get_pandas_df(f"SELECT * FROM {source_table}")
    df.rename(columns=lambda x: x.upper(), inplace=True)  # Ensure column names are uppercase to match Snowflake
    
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()

        # Step 2: Truncate Snowflake table
        cursor.execute(f"TRUNCATE TABLE {target_table}")

        # Step 3: Load data into Snowflake
        success = write_pandas(
            conn=conn,
            df=df,
            table_name=target_table.split('.')[-1],
            schema=target_table.split('.')[-2],
            database=target_table.split('.')[-3],
            quote_identifiers=True,
        )
        assert success, f"Data load to Snowflake failed for table {target_table}"


with DAG(dag_id='test.fullcopy_sqlserver_to_snowflake', tags=['test'], start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    copy_task = PythonOperator(
        task_id='fullcopy_sqlserver_to_snowflake_task',
        python_callable=copy_sqlserver_to_snowflake
    )

    copy_task