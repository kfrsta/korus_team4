import numpy as np
import pandas as pd
import os
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
import re


source_hook = PostgresHook(postgres_conn_id="source_conn")
target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")


def clean_column_names(column_name: str):
    column_name = column_name.lower()
    column_name = column_name.replace(' ', '_')
    column_name = column_name.replace('-', '')
    column_name = column_name.replace('/', '')
    column_name = column_name.replace(', ', '_')
    column_name = column_name.replace('.', '')
    column_name = ''.join(
        char for char in column_name if char.isalnum() or char == '_')
    return column_name


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False
}


@dag(
    dag_id="pipeline_4_andronov",
    default_args=default_args,
    description="main pileline dag",
    schedule_interval="0 4 * * *",
    start_date=datetime(2020, 1, 1),
    catchup=False
)
def taskflow():
    @task
    def load_data_to_ods():
        tables = ["базы_данных", "базы_данных_и_уровень_знаний_сотру", "инструменты", "инструменты_и_уровень_знаний_сотр",
                  "образование_пользователей", "опыт_сотрудника_в_отраслях", "опыт_сотрудника_в_предметных_обла", "отрасли",
                  "платформы", "платформы_и_уровень_знаний_сотруд", "предметная_область", "резюмедар", "сертификаты_пользователей",
                  "сотрудники_дар", "среды_разработки", "среды_разработки_и_уровень_знаний_", "технологии", "технологии_и_уровень_знаний_сотру",
                  "типы_систем", "типы_систем_и_уровень_знаний_сотру", "уровень_образования", "уровни_владения_ин", "уровни_знаний",
                  "уровни_знаний_в_отрасли", "уровни_знаний_в_предметной_област", "фреймворки", "фреймворки_и_уровень_знаний_сотру",
                  "языки", "языки_пользователей", "языки_программирования", "языки_программирования_и_уровень"]
        for table in tables:
            try:
                temp_table = f"temp_{table}"
                create_table_sql = f'CREATE TABLE IF NOT EXISTS andronov_ods."{temp_table}" (LIKE andronov_ods."{table}" INCLUDING ALL);'
                target_hook.run(create_table_sql)
                records = source_hook.get_records(
                    f'SELECT * FROM source_data."{table}";')
                target_hook.insert_rows(
                    f'andronov_ods."{temp_table}"', rows=records)
                drop_table_sql = f'DROP TABLE IF EXISTS andronov_ods."{table}";'
                target_hook.run(drop_table_sql)
                rename_table_sql = f'ALTER TABLE andronov_ods."{temp_table}" RENAME TO "{table}";'
                target_hook.run(rename_table_sql)
            except Exception as e:
                raise AirflowException(
                    f"Failed to load data for table {table}: {e}")

    @task
    def create_dds_scheme():
        try:
            scr_path = "/opt/airflow/scr/sql/create_schema_dds.sql"
            with open(scr_path, "r") as file:
                sql_query = file.read()
            target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")
            target_hook.run(sql_query)
        except Exception as e:
            raise AirflowException(f"Failed to create DDS schema: {e}")

    @task
    def create_broken_schema():
        try:
            scr_path = "/opt/airflow/scr/sql/create_broken_schema.sql"
            with open(scr_path, "r") as file:
                sql_query = file.read()
            target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")
            target_hook.run(sql_query)
        except Exception as e:
            raise AirflowException(f"Failed to create broken schema: {e}")

    @task
    def process_load_dds_measurments_tables():
        measurement_tables = ["базы_данных", "инструменты", "отрасли", "платформы", "предметная_область",
                              "среды_разработки", "технологии", "типы_систем", "уровень_образования", "уровни_владения_ин", "уровни_знаний",
                              "уровни_знаний_в_отрасли", "уровни_знаний_в_предметной_област", "фреймворки", "языки", "языки_программирования"]
        conn = target_hook.get_sqlalchemy_engine()
        try:
            for table in measurement_tables:
                df = pd.read_sql_table(table_name=table, con=conn, schema="andronov_ods")
                df = df[['название', 'id']]
                df.columns = [clean_column_names(x) for x in df.columns]
                truncate_query = f"TRUNCATE TABLE andronov_dds.{table} CASCADE;"
                target_hook.run(truncate_query)
                df.to_sql(name=table, con=conn, if_exists='append', schema="andronov_dds", index=False)
                logging.info(f"Loaded table {table}")
        except Exception as e:
            raise AirflowException(f"Failed to load measurment table: {e}")

    @task
    def process_load_dds_extra_tables():
        extra_tables = ["сотрудники_дар", "сертификаты_пользователей"]
        conn = target_hook.get_sqlalchemy_engine()
        for table in extra_tables:
            df = pd.read_sql_table(table_name=table, con=conn, schema="andronov_ods")
            df = df.drop(columns=['активность', 'Сорт.','Дата изм.'], errors='ignore')
            df.columns = [clean_column_names(x) for x in df.columns]
            truncate_query = f"TRUNCATE TABLE andronov_dds.{table} CASCADE;"
            target_hook.run(truncate_query)
            if 'подразделения' in df.columns:
                df['подразделения'] = df['подразделения'].apply(lambda x: re.sub(r'[^a-zA-Zа-яА-Я\s\/]', '', str(x)))
            if 'год_сертификата' in df.columns:
                df['год_сертификата'] = df['год_сертификата'].fillna(-1)
                df = df[(df['год_сертификата'].astype(int) >= 1990) & (df['год_сертификата'].astype(int) <= datetime.now().year) & (df['год_сертификата'] == '')]
                df_broken = df[(df['год_сертификата'].astype(int) < 1990) | (df['год_сертификата'].astype(int) > datetime.now().year) | (df['год_сертификата'] == '')]
                df_broken.to_sql(name=table, con=conn, if_exists='append', schema="andronov_broken", index=False)
            df.to_sql(name=table, con=conn, if_exists='append',schema="andronov_dds", index=False)
            logging.info(f"Loaded table {table}")


    @task
    def process_load_dds_base_tables():
        base_tables = {"базы_данных_и_уровень_знаний_сотру": ["базы_данных", "уровень_знаний"], "инструменты_и_уровень_знаний_сотр": ["инструменты", "уровень_знаний"],
                       "образование_пользователей": ["уровень_образование"], "опыт_сотрудника_в_отраслях": ["отрасли", "уровень_знаний_в_отрасли"],
                       "опыт_сотрудника_в_предметных_обла": ["предментые_области", "уровень_знаний_в_предметной_облас"], "платформы_и_уровень_знаний_сотруд": ["платформы", "уровень_знаний"],
                       "среды_разработки_и_уровень_знаний_": ["среды_разработки", "уровень_знаний"], "технологии_и_уровень_знаний_сотру": ["технологии", "уровень_знаний"],
                       "типы_систем_и_уровень_знаний_сотру": ["типы_систем", "уровень_знаний"], "фреймворки_и_уровень_знаний_сотру": ["уровень_знаний", "фреймворки"],
                       "языки_пользователей": ["язык", "уровень_знаний_ин_языка"], "языки_программирования_и_уровень": ["уровень_знаний", "языки_программирования"]}
        conn = target_hook.get_sqlalchemy_engine()

        existing_users = pd.read_sql_table(table_name="сотрудники_дар", con=conn, schema="andronov_dds")['id'].unique()

        for table in base_tables.keys():
            df = pd.read_sql_table(
                table_name=table, con=conn, schema="andronov_ods")
            if 'название' in df.columns:
                df = df[df['название'].str.contains('User:')]
                df['название'] = df['название'].apply(lambda x: x.replace('User:', '')).astype(int)
                df = df.rename(columns={'название': 'user_id'})
            df.drop(columns=['активность', 'Сорт.', 'Дата изм.'], inplace=True)
            df.columns = [clean_column_names(x) for x in df.columns]
            truncate_broken = f"TRUNCATE TABLE andronov_broken.{table} CASCADE;"
            truncate_query = f"TRUNCATE TABLE andronov_dds.{table} CASCADE;"
            target_hook.run(truncate_query)
            target_hook.run(truncate_broken)
            df_cleaned = df[df.apply(lambda x: all(x != ''), axis=1)]
            df_broken = df[df.apply(lambda x: any(x == ''), axis=1)]
            for column in base_tables[table]:
                df_cleaned[column] = df_cleaned[column].str.extract(r'\[(\d+)\]').astype(int)
            df_cleaned = df_cleaned[df_cleaned['user_id'].isin(existing_users)]
            df_cleaned.to_sql(name=table, con=conn, if_exists='append', schema="andronov_dds", index=False)
            if len(df_broken) > 0:
                df_broken = df_broken[~df_broken['user_id'].isin(existing_users)]
                df_broken.to_sql(name=table, con=conn, if_exists='append', schema="andronov_broken", index=False)
            logging.info(f"Loaded table {table}")


    t1 = load_data_to_ods()
    t2 = create_dds_scheme()
    t3 = create_broken_schema()
    t4 = process_load_dds_measurments_tables()
    t5 = process_load_dds_extra_tables()
    t6 = process_load_dds_base_tables()

    t1 >> t2 >> t3 >> t4 >> t5 >> t6


main_dag = taskflow()
