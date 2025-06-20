import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.snowflake_utils import get_snowflake_connection


def get_table_list():
    """Get list of all tables - this runs at DAG parse time"""
    mssql_conn_id = os.environ.get('AZURE_SQL_SERVER_CONN_ID')
    database_name = os.environ.get('AZURE_SQL_SERVER_METADATA_DATABASE')
    
    try:
        mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        query = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME 
        FROM {database_name}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        df = mssql_hook.get_pandas_df(query)
        return [(row['TABLE_SCHEMA'], row['TABLE_NAME']) for _, row in df.iterrows()]
    except:
        # Return empty list if connection fails at parse time
        return []


def copy_single_table_task(schema_name, table_name):
    """Create a task function for copying a specific table"""
    def copy_table(**context):
        copy_single_table(schema_name, table_name, **context)
    return copy_table


def copy_single_table(schema_name, table_name, **context):
    """Copy a single table from SQL Server to Snowflake"""
    mssql_conn_id = os.environ.get('AZURE_SQL_SERVER_CONN_ID')
    source_database = os.environ.get('AZURE_SQL_SERVER_METADATA_DATABASE')
    target_database = os.environ.get('SNOWFLAKE_DATABASE')
    
    source_table = f'{source_database}.{schema_name}.{table_name}'
    # Map SQL Server schema to Snowflake schema (you can customize this mapping)
    snowflake_schema = 'VUE' if schema_name.upper() == 'DBO' else schema_name.upper()
    target_table = f'{target_database}.{snowflake_schema}.{table_name.upper()}'
    
    print(f"Copying {source_table} -> {target_table}")
    
    # Step 1: Extract from SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    
    try:
        df = mssql_hook.get_pandas_df(f"SELECT * FROM {source_table}")
        
        if df.empty:
            print(f"Table {source_table} is empty, skipping...")
            return
            
        df.rename(columns=lambda x: x.upper(), inplace=True)  # Ensure column names are uppercase
        
        print(f"Extracted {len(df)} rows from {source_table}")
        
    except Exception as e:
        print(f"Error extracting data from {source_table}: {str(e)}")
        raise
    
    # Step 2: Load to Snowflake
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()

        # Check if table exists and truncate if it does
        check_table_query = f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_CATALOG = '{target_database}' 
        AND TABLE_SCHEMA = '{snowflake_schema}' 
        AND TABLE_NAME = '{table_name.upper()}'
        """
        
        try:
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                cursor.execute(f"TRUNCATE TABLE {target_table}")
                print(f"Table {target_table} exists and has been truncated")
            else:
                print(f"Table {target_table} does not exist, will be created")

            # Load data into Snowflake
            success = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name.upper(),
                schema=snowflake_schema,
                database=target_database,
                quote_identifiers=True,
                auto_create_table=True
            )
            
            if success:
                print(f"Successfully loaded {len(df)} rows to {target_table}")
            else:
                raise Exception(f"Data load to Snowflake failed for table {target_table}")
                
        except Exception as e:
            print(f"Error loading data to {target_table}: {str(e)}")
            raise


with DAG(
    dag_id='fullcopy_all_sqlserver_to_snowflake_dynamic', 
    tags=[os.environ.get('ENVIRONMENT')], 
    start_date=datetime(2025, 1, 1), 
    schedule=None, 
    catchup=False,
    description='Copy all tables from SQL Server to Snowflake with dynamic tasks'
) as dag:
    
    # Get all tables
    tables = get_table_list()
    
    # Create a task for each table
    for schema_name, table_name in tables:
        task_id = f'copy_{schema_name}_{table_name}'.replace('-', '_').replace(' ', '_')
        
        copy_task = PythonOperator(
            task_id=task_id,
            python_callable=copy_single_table_task(schema_name, table_name),
        )