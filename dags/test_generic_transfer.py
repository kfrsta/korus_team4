import datetime as dt

from airflow import DAG
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

source_conn_id = 'source_db'
target_conn_id = 'etl_db_4'
source_schema_name = 'source_data'
target_schema_name = 'dvyacheslav_ods'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}


def create_transfer_tasks(dag):
    source_hook = PostgresHook(postgres_conn_id=f'{source_conn_id}')
    source_cur = source_hook.get_conn().cursor()

    query = f"""SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{source_schema_name}'
                """

    source_cur.execute(query)
    table_names = source_cur.fetchall()

    with TaskGroup("transfer_tasks", dag=dag) as transfer_tasks_group:
        for table in table_names:
            table = table[0]
            transfer_task = GenericTransfer(
                task_id=f'transfer_{table}',
                sql=f"SELECT * FROM {source_schema_name}.{table}",
                source_conn_id=source_conn_id,
                destination_conn_id=target_conn_id,
                destination_table=f"{target_schema_name}.{table}",
                preoperator=f"TRUNCATE TABLE {target_schema_name}.{table}",
                dag=dag
            )
    return transfer_tasks_group


with DAG(dag_id='generic_transfer',
         schedule_interval='@once',
         default_args=default_args) as dag:
    transfer_tasks_group = create_transfer_tasks(dag)
