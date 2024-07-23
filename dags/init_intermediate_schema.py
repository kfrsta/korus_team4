import datetime as dt
import sys

sys.path.append("/opt/airflow/utils")

import pandas as pd
from tables import end_tables, intermediate_tables, names_of_keys_in_the_table
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from sqlalchemy import Integer

target_conn_id = 'etl_db_4'
source_schema_name = 'dvyacheslav_ods'
target_schema_name = 'dvyacheslav_intermediate'
broken_data_schema_name = 'dvyacheslav_broken_data'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}


def migrate_sotrudniki():
    table = 'сотрудники_дар'

    hook = PostgresHook(postgres_conn_id=target_conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as connection:
        df = pd.read_sql_table(table_name=table, con=connection, schema=source_schema_name)
        df = df.rename(columns={'id': 'user_id'})
        # поиск строк с пропусками по любому столбцу
        df_with_missing = df[
            (df['последняя_авторизация'] == "") | (df['должность'] == "") | (df['должность'] == "-") | (
                    df['цфо'] == "")]
        df_without_missing = df[
            (df['последняя_авторизация'] != "") & (df['должность'] != "") & (df['должность'] != "-") & (
                    df['цфо'] != "")]
        # Сохранение DataFrame с пропусками в отдельную схему в базе данных
        df_with_missing.to_sql(name=table, con=connection,
                               schema=broken_data_schema_name, if_exists='replace',
                               index=False)
        # Сохранение DataFrame без пропусков в другую схему в базе данных и предварительная обработка
        df_without_missing.to_sql(name=table, con=connection,
                                  schema=target_schema_name, if_exists='replace', dtype={'user_id': Integer},
                                  index=False)


def create_end_tables():
    table_names = end_tables

    target_hook = PostgresHook(postgres_conn_id=target_conn_id)
    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:
            for table in table_names:
                s = f"""CREATE TABLE IF NOT EXISTS {target_schema_name}.{table} (
                "id" int8, 
                "название" character varying
                );"""

                target_cur.execute(s)


def transfer_data_end_tables():
    table_names = end_tables

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


def migrate_intermediate_tables():
    table_names = intermediate_tables
    table_attributes = names_of_keys_in_the_table

    hook = PostgresHook(postgres_conn_id=target_conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as connection:
        for table in table_names:
            df = pd.read_sql_table(table_name=table, con=connection, schema=source_schema_name)
            df = df.drop(columns=['активность', 'сорт.', 'дата_изм.'])

            # поиск строк с пропусками по любому столбцу
            if table == "образование_пользователей":
                df_with_missing = df[
                    (df['год_окончания'] == 0) | (df['год_окончания'] > 2024) | (df['год_окончания'].isnull())]
                df_without_missing = df[
                    (df['год_окончания'] != 0) & (df['год_окончания'] <= 2024) & ~(df['год_окончания'].isnull())]
            else:
                df_with_missing = df[df.apply(lambda x: any(x == ""), axis=1)]
                df_without_missing = df[~df.apply(lambda x: any(x == ""), axis=1)]

            # Сохранение DataFrame с пропусками в отдельную схему в базе данных
            df_with_missing.to_sql(name=table, con=connection,
                                   schema=broken_data_schema_name, if_exists='replace',
                                   index=False)

            # Сохранение DataFrame без пропусков в другую схему в базе данных и предварительная обработка
            if 'название' in df_without_missing.columns:
                df_without_missing['название'] = df_without_missing['название'].str.extract(r'User:(\d+)').astype(int)
                df_without_missing = df_without_missing.rename(columns={'название': 'user_id'})
            if len(table_attributes[table]) > 0:
                for column in table_attributes[table]:
                    df_without_missing[column] = df_without_missing[column].str.extract(r'\[(\d+)\]').astype(int)
            df_without_missing.to_sql(name=table, con=connection,
                                      schema=target_schema_name, dtype={'user_id': Integer},
                                      if_exists='replace',
                                      index=False)


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

    migrate_sotrudniki = PythonOperator(
        task_id='migrate_table_сотрудники_дар',
        python_callable=migrate_sotrudniki,
        dag=dag,
    )

    create_end_tables_in_schema = PythonOperator(
        task_id='create_end_tables_in_schema',
        python_callable=create_end_tables,
        dag=dag
    )

    migrate_intermediate = PythonOperator(
        task_id='migrate_intermediate_tables',
        python_callable=migrate_intermediate_tables,
        dag=dag,
    )

    create_schema >> migrate_sotrudniki >> create_end_tables_in_schema >> transfer_data_end_tables() >> migrate_intermediate
