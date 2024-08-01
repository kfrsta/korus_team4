import datetime as dt
import sys

sys.path.append("/opt/airflow/utils")

from tables import all_tables, end_tables, intermediate_tables, names_of_keys_in_the_table, names_of_references_tables
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

source_conn_id = 'source_db'
target_conn_id = 'etl_db_4'
source_schema_name = 'source_data'
ods_schema_name = 'dvyacheslav_ods'
intermediate_schema_name = 'dvyacheslav_intermediate'
broken_data_schema_name = 'dvyacheslav_broken_data'
dds_schema_name = 'dvyacheslav_dds'
dm_schema_name = 'dvyacheslav_dm'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}


def create_tables_ods():
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

                create_table_query = f"CREATE TABLE IF NOT EXISTS {ods_schema_name}.{table} ("
                create_table_query += ', '.join([f'"{column_name.lower().replace(" ", "_")}" {data_type}'
                                                 for column_name, data_type in data]) + ');'

                target_cur.execute(create_table_query)


def create_end_tables_dds():
    table_names = end_tables
    target_schema_name = dds_schema_name

    target_hook = PostgresHook(postgres_conn_id=target_conn_id)
    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:
            for table in table_names:
                s = f"""CREATE TABLE IF NOT EXISTS {target_schema_name}.{table} (
                "id" int4 PRIMARY KEY, 
                "название" varchar(100)
                );"""

                target_cur.execute(s)


with DAG(
        dag_id='init_schemes_and_tables',
        description='Создание схем и таблиц со связями в этих схемах',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    with TaskGroup(group_id='ods_schema') as task_group_ods_schema:
        create_schema = SQLExecuteQueryOperator(
            task_id='create_ods_schema',
            sql='sql/create_schema.sql',
            params={"schema_name": ods_schema_name},
            conn_id=target_conn_id
        )

        create_tables_in_ods_schema = PythonOperator(
            task_id='create_tables_in_schema_ods',
            python_callable=create_tables_ods,
            dag=dag
        )

        create_schema >> create_tables_in_ods_schema

    with TaskGroup(group_id='intermediate_schema') as task_group_intermediate_schema:
        create_intermediate_schema = SQLExecuteQueryOperator(
            task_id='create_intermediate_schema',
            sql='sql/create_schema.sql',
            params={"schema_name": intermediate_schema_name},
            conn_id=target_conn_id
        )

        create_broken_schema = SQLExecuteQueryOperator(
            task_id='create_broken_schema',
            sql='sql/create_schema.sql',
            params={"schema_name": broken_data_schema_name},
            conn_id=target_conn_id
        )

        create_intermediate_schema >> create_broken_schema

    with TaskGroup(group_id='dds_schema') as task_group_dds_schema:
        create_schema = SQLExecuteQueryOperator(
            task_id='create_intermediate_schema',
            sql='sql/create_schema.sql',
            params={"schema_name": dds_schema_name},
            conn_id=target_conn_id
        )

        create_end_tables = PythonOperator(
            task_id='create_end_tables_in_dds',
            python_callable=create_end_tables_dds,
            dag=dag
        )

        create_table_sotrudniki = SQLExecuteQueryOperator(
            task_id='create_table_sotrudniki',
            sql='sql/create_table_sotrudniki.sql',
            params={"schema_name": dds_schema_name},
            conn_id=target_conn_id
        )

        create_intermediate_tables = SQLExecuteQueryOperator(
            task_id='create_intermediate_tables_in_dds',
            sql='sql/create_intermediate_tables_in_dds.sql',
            conn_id=target_conn_id
        )

        create_schema >> create_end_tables >> create_table_sotrudniki >> create_intermediate_tables

    with TaskGroup(group_id='dm_schema') as task_group_dm_schema:
        create_schema = SQLExecuteQueryOperator(
            task_id='create_dm_schema',
            sql='sql/create_schema.sql',
            params={"schema_name": dm_schema_name},
            conn_id=target_conn_id
        )

        create_tables = SQLExecuteQueryOperator(
            task_id='create_tables_in_schema_dm',
            sql='sql/create_tables_in_dm_schema.sql',
            conn_id=target_conn_id
        )

        create_schema >> create_tables

    task_group_ods_schema >> task_group_intermediate_schema >> task_group_dds_schema >> task_group_dm_schema
