import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
    'retries': 2,
}

with DAG(dag_id='postgres_dag',  # уникальный id
         schedule_interval='@once',  # расписание запусков
         default_args=default_args) as dag:
    test_sql = PostgresOperator(
        task_id='select_limit_1',
        postgres_conn_id='source_db',
        sql="SELECT * FROM source.source_data.базы_данных LIMIT 1",
        dag=dag,
    )
