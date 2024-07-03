from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_ods_schema_and_tables',
    default_args=default_args,
    description='Create ODS schema and tables based on source schema',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)


def get_tables():
    source_hook = PostgresHook(postgres_conn_id='source_conn')
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'source_data'
        AND table_type = 'BASE TABLE';
    """
    records = source_hook.get_records(query)
    return [record[0] for record in records]


def create_ods_schema_and_tables():
    target_hook = PostgresHook(postgres_conn_id='etl_db_4_conn')

    target_hook.run("CREATE SCHEMA IF NOT EXISTS ods;")

    tables = get_tables()

    for table in tables:
        target_hook.run(f"DROP TABLE IF EXISTS ods.{table}")
        create_table_query = f'''
            CREATE TABLE ods.{table} (
                LIKE source_data.{table} INCLUDING ALL
            );
        '''
        target_hook.run(create_table_query)


create_ods_schema_and_tables_task = PythonOperator(
    task_id='create_ods_schema_and_tables',
    python_callable=create_ods_schema_and_tables,
    dag=dag,
)


