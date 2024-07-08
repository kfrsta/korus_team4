from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


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


def load_data_to_ods():
    source_hook = PostgresHook(postgres_conn_id='source_conn')
    target_hook = PostgresHook(postgres_conn_id='etl_db_4_conn')

    tables = get_tables()

    for table in tables:
        select_query = f"SELECT * FROM source.source_data.{table}"
        records = source_hook.get_records(select_query)
        target_hook.insert_rows(f'etl_db_4.ods.{table}', rows=records)


with DAG(
    'ods_load',
    default_args=default_args,
    description='Load data to ODS schema',
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False
) as dag:
    load_data_ods_task = PythonOperator(
        task_id='load_data_to_ods',
        python_callable=load_data_to_ods
    )
