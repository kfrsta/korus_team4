from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from ods_init import get_tables


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_data_to_ods',
    default_args=default_args,
    description='Load data to ODS schema',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)


def load_data_to_ods():
    source_hook = PostgresHook(postgres_conn_id='source_postgres')
    target_hook = PostgresHook(postgres_conn_id='target_postgres')

    tables = get_tables()

    for table in tables:
        select_query = f"SELECT * FROM public.{table}"
        records = source_hook.get_records(select_query)
        target_hook.insert_rows(f'ods.{table}', rows=records)


load_data_to_ods_task = PythonOperator(
    task_id='load_data_to_ods',
    python_callable=load_data_to_ods,
    dag=dag,
)
