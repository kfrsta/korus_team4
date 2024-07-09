import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
    'retries': 2,
}

with DAG(
        dag_id='create_schema',
        schedule_interval='@once',
        default_args=default_args,
) as dag:
    create_table = PostgresOperator(
        task_id='source_data',
        sql='sql/create_schema.sql',
        postgres_conn_id='etl_db_4',
    )
