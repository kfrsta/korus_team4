import datetime as dt
import sys

sys.path.append("/opt/airflow/utils")

from tables import end_tables
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

source_conn_id = 'source_db'
target_conn_id = 'etl_db_4'
source_schema_name = 'dvyacheslav_ods'
target_schema_name = 'dvyacheslav_intermediate'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}

table_names = end_tables


def create_end_tables():
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:
            for table in table_names:
                s = f"""CREATE TABLE IF NOT EXISTS {target_schema_name}.{table} (
                "id" int8, 
                "название" character varying
                );"""

                target_cur.execute(s)


def transfer_data():
    with TaskGroup(group_id="transfer_data_end_tables") as transfer_tasks_group:
        for table in table_names:
            query = f"""SELECT "id", "название" FROM {source_schema_name}.{table};"""

            transfer_task = GenericTransfer(
                task_id=f'migrate_from_{table}',
                sql=query,
                source_conn_id=target_conn_id,
                destination_conn_id=target_conn_id,
                destination_table=f"{target_schema_name}.{table}",
                preoperator=f"TRUNCATE TABLE {target_schema_name}.{table}",
                dag=dag
            )
    return transfer_tasks_group


with DAG(
        dag_id='migrate_end_tables',
        description='Перенос конечных таблиц с их содержимым в промежуточный слой',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    create_schema = PostgresOperator(
        task_id='create_intermediate_schema',
        sql='sql/create_intermediate_schema.sql',
        postgres_conn_id=target_conn_id,
    )

    create_end_tables_in_schema = PythonOperator(
        task_id='create_end_tables_in_schema',
        python_callable=create_end_tables,
        dag=dag
    )

    create_schema >> create_end_tables_in_schema >> transfer_data()
