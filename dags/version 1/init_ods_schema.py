import datetime as dt
import sys

sys.path.append("/opt/airflow/utils")

from tables import all_tables
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
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


def create_tables():
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    source_cur = source_hook.get_conn().cursor()

    table_names = all_tables

    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:
            for table in table_names:
                query = f"""SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table}';"""

                source_cur.execute(query)
                data = source_cur.fetchall()

                create_table_query = f"CREATE TABLE IF NOT EXISTS {target_schema_name}.{table} ("
                create_table_query += ', '.join([f'"{column_name.lower().replace(" ", "_")}" {data_type}'
                                                 for column_name, data_type in data]) + ');'

                target_cur.execute(create_table_query)


def transfer_data():
    table_names = all_tables

    with TaskGroup(group_id="transfer_tasks") as transfer_tasks_group:
        for table in table_names:
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


with DAG(
        dag_id='init_ods_schema',
        description='Загрузка исходных данных в ods схему',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        sql='sql/create_schema.sql',
        params={"schema_name": target_schema_name},
        conn_id=target_conn_id
    )

    create_tables_in_schema = PythonOperator(
        task_id='create_tables_in_schema',
        python_callable=create_tables,
        dag=dag
    )

    create_schema >> create_tables_in_schema >> transfer_data()
