import datetime as dt
import sys

sys.path.append("/opt/airflow/utils")

import pandas as pd
from tables import all_tables, end_tables, intermediate_tables, names_of_keys_in_the_table
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from sqlalchemy.types import Integer, String, Date, DateTime

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


def transfer_data_ods():
    table_names = all_tables
    target_schema_name = ods_schema_name

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


def migrate_sotrudniki_intr():
    table = 'сотрудники_дар'
    target_schema_name = intermediate_schema_name
    source_schema_name = ods_schema_name

    hook = PostgresHook(postgres_conn_id=target_conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as connection:
        df = pd.read_sql_table(table_name=table, con=connection, schema=source_schema_name)
        df = df.rename(columns={'id': 'user_id'})
        df = df.drop(columns=['дата_рождения', 'пол', 'дата_регистрации', 'дата_изменения', 'e-mail', 'логин', 'компания',
                         'город_проживания'])
        df_with_missing = df[
            (df['последняя_авторизация'] == "") | (df['должность'] == "") | (df['должность'] == "-") | (
                    df['цфо'] == "")]
        df_without_missing = df[
            (df['последняя_авторизация'] != "") & (df['должность'] != "") & (df['должность'] != "-") & (
                    df['цфо'] != "")]
        # Сохранение DataFrame 'сотрудники_дар' с пропусками в отдельную схему в базе данных
        df_with_missing.to_sql(name=table, con=connection,
                               schema=broken_data_schema_name, if_exists='replace',
                               index=False)
        # Сохранение DataFrame 'сотрудники_дар' без пропусков в intermediate схему в базе данных
        df_without_missing.to_sql(name=table, con=connection,
                                  schema=target_schema_name, if_exists='replace',
                                  dtype={'user_id': Integer, 'последняя_авторизация': DateTime},
                                  index=False)


def migrate_end_tables_intr():
    table_names = end_tables
    target_schema_name = intermediate_schema_name
    source_schema_name = ods_schema_name

    hook = PostgresHook(postgres_conn_id=target_conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as connection:
        for table in table_names:
            df = pd.read_sql_table(table_name=table, con=connection, schema=source_schema_name)
            df = df.drop(columns=['активность', 'сорт.', 'дата_изм.'])
            df = df[['id', 'название']]
            df.to_sql(name=table, con=connection, schema=target_schema_name,
                      dtype={'id': Integer, 'название': String(100)},
                      if_exists='replace',
                      index=False)


def migrate_intermediate_tables_intr():
    table_names = intermediate_tables
    table_attributes = names_of_keys_in_the_table
    target_schema_name = intermediate_schema_name
    source_schema_name = ods_schema_name

    hook = PostgresHook(postgres_conn_id=target_conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as connection:
        for table in table_names:
            df = pd.read_sql_table(table_name=table, con=connection, schema=source_schema_name)

            kwargs_to_sql = {'name': table, 'con': connection, 'schema': target_schema_name,
                             'dtype': {'user_id': Integer, 'дата': Date}, 'if_exists': 'replace', 'index': False}

            df = df.drop(columns=['активность', 'сорт.', 'дата_изм.'])
            # обработка отдельных таблиц
            if table == "образование_пользователей":
                df = df.rename(columns={'факультет,_кафедра': 'факультет_кафедра'})
                df_with_missing = df[
                    (df['год_окончания'] == 0) | (df['год_окончания'] > 2024) | (df['год_окончания'].isnull())]

                df_without_missing = df[
                    (df['год_окончания'] != 0) & (df['год_окончания'] <= 2024) & ~(df['год_окончания'].isnull())]

                column = table_attributes[table][0]
                df_without_missing[column] = df_without_missing[column].str.extract(r'\[(\d+)\]').astype(int)
                kwargs_to_sql['dtype'][column] = Integer
                kwargs_to_sql['dtype']['год_окончания'] = Integer
            elif table == "сертификаты_пользователей":
                df = df.rename(columns={'организация,_выдавшая_сертификат': 'организация_выдавшая_сертификат'})
                df_with_missing = df[
                    (df['год_сертификата'] == 0) | (df['год_сертификата'] > 2024) | (df['год_сертификата'].isnull())]
                df_with_missing = df_with_missing[df_with_missing.apply(lambda x: any(x == ""), axis=1)]

                df_without_missing = df[
                    (df['год_сертификата'] != 0) & (df['год_сертификата'] <= 2024) & ~(df['год_сертификата'].isnull())]
                df_without_missing = df_without_missing[~df_without_missing.apply(lambda x: any(x == ""), axis=1)]
                kwargs_to_sql['dtype']['год_сертификата'] = Integer
            else:
                if table == "языки_пользователей":
                    df = df.rename(columns={'уровень_знаний_ин._языка': 'уровень_знаний_ин_языка'})
                df_with_missing = df[df.apply(lambda x: any(x == ""), axis=1)]

                df_without_missing = df[~df.apply(lambda x: any(x == ""), axis=1)]
                if 'название' in df_without_missing.columns:
                    df_without_missing['название'] = df_without_missing['название'].str.extract(r'User:(\d+)').astype(
                        int)
                    df_without_missing = df_without_missing.rename(columns={'название': 'user_id'})
                # df_without_missing = df_without_missing[
                #     ['id', 'user_id', table_attributes[table][0], table_attributes[table][1]], 'дата']
                for column in table_attributes[table]:
                    df_without_missing[column] = df_without_missing[column].str.extract(r'\[(\d+)\]').astype(int)
                    kwargs_to_sql['dtype'][column] = Integer
                df_without_missing = df_without_missing.drop_duplicates(subset=['user_id'] + table_attributes[table])

            # Сохранение DataFrame с пропусками в отдельную схему в базе данных
            df_with_missing.to_sql(name=table, con=connection,
                                   schema=broken_data_schema_name, if_exists='replace',
                                   index=False)

            # Сохранение DataFrame без пропусков в другую схему в базе данных
            df_without_missing.to_sql(**kwargs_to_sql)


def transfer_data_end_tables_dds():
    table_names = end_tables
    source_schema_name = intermediate_schema_name
    target_schema_name = dds_schema_name

    with TaskGroup(group_id="transfer_data_end_tables") as transfer_tasks_group:
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


def transfer_data_intermediate_tables_dds():
    table_names = intermediate_tables
    source_schema_name = intermediate_schema_name
    target_schema_name = dds_schema_name

    with TaskGroup(group_id="transfer_data_intermediate_tables") as transfer_tasks_group:
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
        dag_id='update_data',
        description='Даг для обновления данных в созданных схемах',
        schedule_interval='0 4 * * *',
        catchup=False,
        default_args=default_args,
) as dag:
    with TaskGroup(group_id='intermediate_schema') as task_group_intermediate_schema:
        migrate_sotrudniki = PythonOperator(
            task_id='migrate_table_сотрудники_дар',
            python_callable=migrate_sotrudniki_intr,
            dag=dag,
        )

        migrate_end = PythonOperator(
            task_id='migrate_end_tables',
            python_callable=migrate_end_tables_intr,
            dag=dag,
        )

        migrate_intermediate = PythonOperator(
            task_id='migrate_intermediate_tables',
            python_callable=migrate_intermediate_tables_intr,
            dag=dag,
        )

        migrate_sotrudniki >> migrate_end >> migrate_intermediate

    with TaskGroup(group_id='dds_schema') as task_group_dds_schema:
        migrate_sotrudniki = GenericTransfer(
            task_id=f'migrate_table_сотрудники_дар',
            sql=f"""SELECT * FROM {intermediate_schema_name}.сотрудники_дар;""",
            source_conn_id=target_conn_id,
            destination_conn_id=target_conn_id,
            destination_table=f"{dds_schema_name}.сотрудники_дар",
            preoperator=f"TRUNCATE TABLE {dds_schema_name}.сотрудники_дар CASCADE",
            dag=dag
        )

        migrate_sotrudniki >> transfer_data_end_tables_dds() >> transfer_data_intermediate_tables_dds()

    with TaskGroup(group_id='dm_schema') as task_group_dm_schema:
        migrate_knowledge_levels = GenericTransfer(
            task_id=f'migrate_knowledge_level_table',
            sql=f"""SELECT * FROM {dds_schema_name}.уровни_знаний;""",
            source_conn_id=target_conn_id,
            destination_conn_id=target_conn_id,
            destination_table=f"{dm_schema_name}.knowledge_levels",
            preoperator=f"TRUNCATE TABLE {dm_schema_name}.knowledge_levels CASCADE",
            dag=dag
        )

        migrate_users = GenericTransfer(
            task_id=f'migrate_users_table',
            sql=f"""SELECT * FROM {dds_schema_name}.сотрудники_дар;""",
            source_conn_id=target_conn_id,
            destination_conn_id=target_conn_id,
            destination_table=f"{dm_schema_name}.users",
            preoperator=f"TRUNCATE TABLE {dm_schema_name}.users CASCADE",
            dag=dag
        )

        migrate_fact_table = SQLExecuteQueryOperator(
            task_id='migrate_fact_employee_skills',
            sql='sql/filling_fact_tables.sql',
            conn_id=target_conn_id,
        )

        migrate_knowledge_levels >> migrate_users >> migrate_fact_table

    transfer_data_ods() >> task_group_intermediate_schema >> task_group_dds_schema >> task_group_dm_schema
