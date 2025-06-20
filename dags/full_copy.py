import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.snowflake_utils import get_snowflake_connection


def copy_sqlserver_to_snowflake(**context):
    mssql_conn_id = os.environ.get('AZURE_SQL_SERVER_CONN_ID')
    source_table = f'{os.environ.get('AZURE_SQL_SERVER_METADATA_DATABASE')}.dbo.Features'
    target_table = f'{os.environ.get('SNOWFLAKE_DATABASE')}.VUE.FEATURES'
    
    # Step 1: Extract from SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    df = mssql_hook.get_pandas_df(f"SELECT * FROM {source_table}")
    df.rename(columns=lambda x: x.upper(), inplace=True)  # Ensure column names are uppercase to match Snowflake
    
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()

        # Step 2: Check if table exists and truncate if it does
        database_name = target_table.split('.')[0]
        schema_name = target_table.split('.')[1]
        table_name = target_table.split('.')[2]
        
        check_table_query = f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_CATALOG = '{database_name}' 
        AND TABLE_SCHEMA = '{schema_name}' 
        AND TABLE_NAME = '{table_name}'
        """
        
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            cursor.execute(f"TRUNCATE TABLE {target_table}")
            print(f"Table {target_table} exists and has been truncated")
        else:
            print(f"Table {target_table} does not exist, skipping truncate")

        # Step 3: Load data into Snowflake
        success = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema_name,
            database=database_name,
            quote_identifiers=True,
            auto_create_table=True
        )
        assert success, f"Data load to Snowflake failed for table {target_table}"


with DAG(dag_id='fullcopy_sqlserver_to_snowflake', tags=[os.environ.get('ENVIRONMENT')], start_date=datetime(2025, 1, 1), schedule=None, catchup=False) as dag:
    copy_task = PythonOperator(
        task_id='fullcopy_sqlserver_to_snowflake_task',
        python_callable=copy_sqlserver_to_snowflake
    )

    copy_task