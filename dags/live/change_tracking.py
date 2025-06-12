from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


def sync_changes_to_snowflake(**context):
    table_schema = 'dbo'
    table_name = 'Features'
    pk_column = 'Id'
    last_change_version = int(Variable.get('last_change_version', default=0))

    sql = f"""
        SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, ct.Id, t.*
        FROM CHANGETABLE(CHANGES {table_schema}.{table_name}, {last_change_version}) AS ct
        LEFT JOIN {table_schema}.{table_name} AS t
          ON t.{pk_column} = ct.{pk_column}
    """
    mssql_hook = MsSqlHook(mssql_conn_id='local-ssms')
    results = mssql_hook.get_records(sql)

    if not results:
        print("No changes to sync.")
        return

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    snowflake_table = "vue.Features"
    
    delete_count = 0
    upsert_count = 0

    # Track the **maximum** SYS_CHANGE_VERSION found in this run
    max_change_version = last_change_version

    for row in results:
        sys_change_version, sys_change_operation, pk_value, *rest = row

        # Track the highest change version
        if sys_change_version > max_change_version:
            max_change_version = sys_change_version

        if sys_change_operation == 'D':
            delete_stmt = f"DELETE FROM {snowflake_table} WHERE {pk_column} = %s"
            cursor.execute(delete_stmt, (pk_value,))
            delete_count += cursor.rowcount
        else:
            upsert_stmt = f"""
                MERGE INTO {snowflake_table} t
                USING (SELECT %s AS Id) src
                ON t.Id = src.Id
                WHEN MATCHED THEN
                UPDATE SET
                    Name = %s,
                    DocumentationUrl = %s,
                    FeatureCode = %s,
                    IsActive = %s
                WHEN NOT MATCHED THEN
                INSERT (Id, Name, DocumentationUrl, FeatureCode, IsActive)
                VALUES (%s, %s, %s, %s, %s)
            """
            # rest = [Id, Name, DocumentationUrl, FeatureCode, IsActive]
            values = (
                pk_value,
                rest[1], rest[2], rest[3], rest[4],
                pk_value, rest[1], rest[2], rest[3], rest[4]
            )
            cursor.execute(upsert_stmt, values)
            upsert_count += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Upserts: {upsert_count}, Deletes: {delete_count}")

    # After processing, store the latest processed version
    if max_change_version > last_change_version:
        Variable.set('last_change_version', str(max_change_version))

with DAG(dag_id='live.sync_sqlserver_to_snowflake', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    sync_task = PythonOperator(
        task_id='sync_changes_to_snowflake',
        python_callable=sync_changes_to_snowflake
    )