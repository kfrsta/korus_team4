import datetime as dt
import sys

sys.path.append("/opt/airflow/utils")

import pandas as pd
from tables import end_tables, intermediate_tables, names_of_keys_in_the_table
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.types import Integer, String, Date, DateTime

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


def migrate_end_tables():
    table_names = end_tables

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


def migrate_intermediate_tables():
    table_names = intermediate_tables
    table_attributes = names_of_keys_in_the_table

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


with DAG(
        dag_id='init_intermediate_schema',
        description='Загрузка обработанных данных в intermediate схему',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    create_intermediate_schema = SQLExecuteQueryOperator(
        task_id='create_intermediate_schema',
        sql='sql/create_schema.sql',
        params={"schema_name": target_schema_name},
        conn_id=target_conn_id
    )

    create_broken_schema = SQLExecuteQueryOperator(
        task_id='create_broken_schema',
        sql='sql/create_schema.sql',
        params={"schema_name": broken_data_schema_name},
        conn_id=target_conn_id
    )

    migrate_sotrudniki = PythonOperator(
        task_id='migrate_table_сотрудники_дар',
        python_callable=migrate_sotrudniki,
        dag=dag,
    )

    migrate_end = PythonOperator(
        task_id='migrate_end_tables',
        python_callable=migrate_end_tables,
        dag=dag,
    )

    migrate_intermediate = PythonOperator(
        task_id='migrate_intermediate_tables',
        python_callable=migrate_intermediate_tables,
        dag=dag,
    )

    create_intermediate_schema >> create_broken_schema >> migrate_sotrudniki >> migrate_end >> migrate_intermediate
