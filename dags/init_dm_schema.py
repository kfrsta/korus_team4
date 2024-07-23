import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.utils.task_group import TaskGroup

source_conn_id = 'source_db'
target_conn_id = 'etl_db_4'
source_schema_name = 'dvyacheslav_dds'
target_schema_name = 'dvyacheslav_dm'
# broken_data_schema_name = 'dvyacheslav_broken_data'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}

# end_tables = ['сотрудники_дар', 'фреймворки', 'инструменты', 'уровни_знаний', 'среды_разработки']
#
# intermediate_tables = ['среды_разработки_и_уровень_знаний_', 'инструменты_и_уровень_знаний_сотр',
#                        'фреймворки_и_уровень_знаний_сотру', 'сертификаты_пользователей']
#
#
# def migrate_end_tables():
#     with TaskGroup(group_id="migrate_end_data") as transfer_tasks_group:
#         for table in end_tables:
#             query = f"""SELECT * FROM {source_schema_name}.{table};"""
#
#             transfer_task = GenericTransfer(
#                 task_id=f'migrate_from_{table}',
#                 sql=query,
#                 source_conn_id=target_conn_id,
#                 destination_conn_id=target_conn_id,
#                 destination_table=f"{target_schema_name}.{table}",
#                 preoperator=f"TRUNCATE TABLE {target_schema_name}.{table} CASCADE",
#                 dag=dag
#             )
#         return transfer_tasks_group
#
#
# def migrate_intermediate_tables():
#     with TaskGroup(group_id="migrate_intermediate_data") as transfer_tasks_group:
#         for table in intermediate_tables:
#             query = f"""SELECT * FROM {source_schema_name}.{table};"""
#
#             transfer_task = GenericTransfer(
#                 task_id=f'migrate_from_{table}',
#                 sql=query,
#                 source_conn_id=target_conn_id,
#                 destination_conn_id=target_conn_id,
#                 destination_table=f"{target_schema_name}.{table}",
#                 preoperator=f"TRUNCATE TABLE {target_schema_name}.{table}",
#                 dag=dag
#             )
#         return transfer_tasks_group


with DAG(
        dag_id='init_dm_schema',
        description='Создание dm схемы со справочными таблицами',
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

    migrate_certificate = GenericTransfer(
        task_id=f'migrate_certificate_table',
        sql=f"""
            SELECT id, наименование_сертификата, 'организация,_выдавшая_сертификат' 
            FROM {source_schema_name}.сертификаты_пользователей;""",
        source_conn_id=target_conn_id,
        destination_conn_id=target_conn_id,
        destination_table=f"{target_schema_name}.sertificates",
        preoperator=f"TRUNCATE TABLE {target_schema_name}.sertificates CASCADE",
        dag=dag
    )

    migrate_fact_table = PostgresOperator(
        task_id='migrate_fact_employee_skills',
        sql='sql/filling_fact_tables.sql',
        postgres_conn_id=target_conn_id,
    )

    create_schema_and_tables >> migrate_knowledge_levels >> migrate_users >> migrate_certificate >> migrate_fact_table
