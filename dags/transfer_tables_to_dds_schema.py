import datetime as dt
import sys

sys.path.append("/opt/airflow/utils")

from tables import end_tables, intermediate_tables, names_of_keys_in_the_table, names_of_references_tables
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.generic_transfer import GenericTransfer
from airflow.utils.task_group import TaskGroup

source_conn_id = 'source_db'
target_conn_id = 'etl_db_4'
source_schema_name = 'dvyacheslav_intermediate'
target_schema_name = 'dvyacheslav_dds'
broken_data_schema_name = 'dvyacheslav_broken_data'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}


def migrate_tables_16():
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    table_names = end_tables

    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:
            for table in table_names:
                s = f"""CREATE TABLE IF NOT EXISTS {target_schema_name}.{table} (
                "id" int4 PRIMARY KEY, 
                "название" character varying
                );"""  # возможно, здесь не стоит указывать PRIMARY KEY

                target_cur.execute(s)


def migrate_other_tables():
    table_names = end_tables

    with TaskGroup(group_id="migrate_other_tables") as transfer_tasks_group:
        for table in table_names:
            query = f"""SELECT * FROM {source_schema_name}.{table};"""

            transfer_task = GenericTransfer(
                task_id=f'migrate_from_{table}',
                sql=query,
                source_conn_id=target_conn_id,
                destination_conn_id=target_conn_id,
                destination_table=f"{target_schema_name}.{table}",
                preoperator=f"TRUNCATE TABLE {target_schema_name}.{table} CASCADE",
                dag=dag
            )
        return transfer_tasks_group


def migrate():
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    table_names = intermediate_tables

    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:
            for table in table_names:  # здесь идет обработка таблиц, у которых 3 foreign key атрибута
                query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table}' AND table_schema = '{source_schema_name}';"""

                target_cur.execute(query)
                data = target_cur.fetchall()

                create_table_query = f"CREATE TABLE IF NOT EXISTS {target_schema_name}.{table} ("
                for column_name, data_type in data:
                    create_table_query += f'"{column_name}" {data_type}, '
                create_table_query += f'''
                FOREIGN KEY (user_id) REFERENCES {target_schema_name}.сотрудники_дар (user_id)'''
                if len(names_of_keys_in_the_table[table]) == 2:
                    create_table_query += f'''
                    , FOREIGN KEY ("{names_of_keys_in_the_table[table][0]}") REFERENCES {target_schema_name}.{names_of_references_tables[table][0]} (id),
                    FOREIGN KEY ("{names_of_keys_in_the_table[table][1]}") REFERENCES {target_schema_name}.{names_of_references_tables[table][1]} (id)
                    );'''
                elif len(names_of_keys_in_the_table[table]) == 1:
                    create_table_query += f'''
                    , FOREIGN KEY ("{names_of_keys_in_the_table[table][0]}") REFERENCES {target_schema_name}.{names_of_references_tables[table][0]} (id)
                    );'''
                else:
                    create_table_query += ');'
                target_cur.execute(create_table_query)


def migrate_information():
    table_names = intermediate_tables

    with TaskGroup(group_id="migrate_information") as transfer_tasks_group:
        for table in table_names:
            transfer_task = GenericTransfer(
                task_id=f'migrate_from_{table}',
                sql=f"""
                SELECT * 
                FROM {source_schema_name}.{table}
                WHERE user_id IN (SELECT user_id FROM {source_schema_name}.сотрудники_дар);""",
                source_conn_id=target_conn_id,
                destination_conn_id=target_conn_id,
                destination_table=f"{target_schema_name}.{table}",
                preoperator=f"""TRUNCATE TABLE {target_schema_name}.{table};""",
                dag=dag
            )
    return transfer_tasks_group


with DAG(
        dag_id='test_transfer_to_dds',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    create_schema = PostgresOperator(
        task_id='create_dds_schema',
        sql='sql/create_dds_schema.sql',
        postgres_conn_id=target_conn_id,
    )

    migrate_sotrudniki = GenericTransfer(
        task_id=f'migrate_from_сотрудники_дар',
        sql=f"""SELECT * FROM {source_schema_name}.сотрудники_дар;""",
        source_conn_id=target_conn_id,
        destination_conn_id=target_conn_id,
        destination_table=f"{target_schema_name}.сотрудники_дар",
        preoperator=f"TRUNCATE TABLE {target_schema_name}.сотрудники_дар CASCADE",
        dag=dag
    )

    extract_and_insert_tables = PythonOperator(
        task_id='migrate_tables_16',
        python_callable=migrate_tables_16,
        dag=dag
    )

    transfer_tasks_group = migrate_other_tables()

    # создали таблицу
    test_create_dds_table = PythonOperator(
        task_id='test_create_dds_table',
        python_callable=migrate,
        dag=dag
    )

    # transfer_broken_resume = PostgresOperator(
    #     task_id='transfer_broken_resume',
    #     postgres_conn_id=target_conn_id,
    #     sql=f"""
    #         INSERT INTO {broken_data_schema_name}.{table}
    #         SELECT *
    #         FROM {source_schema_name}.{table}
    #         WHERE user_id NOT IN (SELECT user_id FROM {source_schema_name}.сотрудники_дар);
    #         """
    # )

    migrate_resume = migrate_information()

    create_schema >> migrate_sotrudniki >> extract_and_insert_tables >> transfer_tasks_group >> test_create_dds_table >> migrate_resume
