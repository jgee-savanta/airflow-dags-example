from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(dag_id='live.test_azure_mssql_connection', tags=['live'], start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    task = SQLExecuteQueryOperator(
        task_id='run_select',
        sql="SELECT GETDATE();",
        conn_id='azure_sql_vm'
    )