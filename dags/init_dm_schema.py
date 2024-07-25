import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer

target_conn_id = 'etl_db_4'
source_schema_name = 'dvyacheslav_dds'
target_schema_name = 'dvyacheslav_dm'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}

with DAG(
        dag_id='init_dm_schema',
        description='Создание dm схемы в форме звезды',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    create_schema_and_tables = PostgresOperator(
        task_id='create_dm_schema_and_tables',
        sql='sql/create_dm_schema.sql',
        postgres_conn_id=target_conn_id,
    )

    migrate_knowledge_levels = GenericTransfer(
        task_id=f'migrate_knowledge_level_table',
        sql=f"""SELECT * FROM {source_schema_name}.уровни_знаний;""",
        source_conn_id=target_conn_id,
        destination_conn_id=target_conn_id,
        destination_table=f"{target_schema_name}.knowledge_levels",
        preoperator=f"TRUNCATE TABLE {target_schema_name}.knowledge_levels CASCADE",
        dag=dag
    )

    migrate_users = GenericTransfer(
        task_id=f'migrate_users_table',
        sql=f"""SELECT * FROM {source_schema_name}.сотрудники_дар;""",
        source_conn_id=target_conn_id,
        destination_conn_id=target_conn_id,
        destination_table=f"{target_schema_name}.users",
        preoperator=f"TRUNCATE TABLE {target_schema_name}.users CASCADE",
        dag=dag
    )

    migrate_fact_table = PostgresOperator(
        task_id='migrate_fact_employee_skills',
        sql='sql/filling_fact_tables.sql',
        postgres_conn_id=target_conn_id,
    )

    create_schema_and_tables >> migrate_knowledge_levels >> migrate_users >> migrate_fact_table
