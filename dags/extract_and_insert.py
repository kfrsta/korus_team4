import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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


def get_data_from_source_data():
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    source_cur = source_hook.get_conn().cursor()

    query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{source_schema_name}'
            """

    source_cur.execute(query)
    table_names = source_cur.fetchall()

    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:

            for table in table_names:
                table = table[0]
                query = f"""SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table}';"""

                source_cur.execute(query)
                data = source_cur.fetchall()

                s = f"CREATE TABLE IF NOT EXISTS {target_schema_name}.{table} ("
                for column_name, data_type in data:
                    column_name = column_name.lower().replace(' ', '_')
                    s += f'"{column_name}" {data_type}, '
                create_table_query = s.rstrip(', ') + ');'

                target_cur.execute(create_table_query)


def create_transfer_tasks():
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    source_cur = source_hook.get_conn().cursor()

    query = f"""SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{source_schema_name}'
                """

    source_cur.execute(query)
    table_names = source_cur.fetchall()

    with TaskGroup(group_id="transfer_tasks") as transfer_tasks_group:
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


with DAG(
        dag_id='extract_and_insert',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    create_schema = PostgresOperator(
        task_id='create_schema',
        sql='sql/create_schema.sql',
        postgres_conn_id=target_conn_id,
    )

    extract_and_insert_tables = PythonOperator(
        task_id='extract_data',
        python_callable=get_data_from_source_data,
        dag=dag
    )

    transfer_tasks_group = create_transfer_tasks()

    create_schema >> extract_and_insert_tables >> transfer_tasks_group
