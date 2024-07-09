import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

source_conn_id = 'source_db'
target_conn_id = 'etl_db_4'
schema_name = 'source_data'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}


def get_data_from_source_data():
    source_hook = PostgresHook(postgres_conn_id=f'{source_conn_id}')
    source_cur = source_hook.get_conn().cursor()

    query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            """

    source_cur.execute(query)
    table_names = source_cur.fetchall()

    target_hook = PostgresHook(postgres_conn_id=f'{target_conn_id}')

    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:

            for table in table_names:
                table = table[0]
                query = f"""SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table}';"""

                source_cur.execute(query)
                data = source_cur.fetchall()

                s = f"CREATE TABLE IF NOT EXISTS dvyacheslav_ods.{table} ("
                for column_name, data_type in data:
                    s += f'"{column_name}" {data_type}, '
                create_table_query = s.rstrip(', ') + ');'

                target_cur.execute(create_table_query)


with DAG(
        dag_id='extract_and_insert',
        schedule_interval='@once',
        default_args=default_args,
) as dag:
    create_table = PythonOperator(
        task_id='extract_data',
        python_callable=get_data_from_source_data,
        dag=dag
    )
