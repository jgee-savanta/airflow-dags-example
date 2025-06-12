from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime


def copy_sqlserver_to_snowflake(**context):
    # Connections (refer to Airflow Connection IDs)
    mssql_conn_id = 'local-ssms'
    snowflake_conn_id = 'snowflake'
    
    source_table = 'dbo.Features'
    target_table = 'BrandVueMeta_Test.vue.Features'.upper()
    
    # Step 1: Extract from SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    df = mssql_hook.get_pandas_df(f"SELECT * FROM {source_table}")
    df.rename(columns=lambda x: x.upper(), inplace=True)  # Ensure column names are uppercase to match Snowflake
    
    # Step 2: Truncate Snowflake table
    sf_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    truncate_sql = f"TRUNCATE TABLE {target_table}"
    sf_hook.run(truncate_sql)
    
    # Step 3: Load data into Snowflake
    # We'll use Snowflake's write_pandas for efficiency:
    session = sf_hook.get_snowpark_session()
    success = session.write_pandas(
        df=df,
        table_name=target_table.split('.')[-1],      # Table name without schema/db
        schema=target_table.split('.')[-2],          # Schema name
        database=target_table.split('.')[-3],        # Database name
        quote_identifiers=True,
        # auto_create_table=True, 
    )
    assert success, f"Data load to Snowflake failed for table {target_table}"

with DAG('beta.fullcopy_sqlserver_to_snowflake', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    copy_task = PythonOperator(
        task_id='fullcopy_sqlserver_to_snowflake_task',
        python_callable=copy_sqlserver_to_snowflake
    )

    copy_task