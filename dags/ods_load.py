import numpy as np
import pandas as pd
import json
import os
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
import sys
import re
import shutil
sys.path.append("/opt/airflow/scr")
from preprocess_utils import get_pd_df, clean_table_df, clean_column_names, create_intermediate_table

temp_dir = "/opt/airflow/temp"

mapping_file = "/opt/airflow/table_config/dds_config.json"

source_hook = PostgresHook(postgres_conn_id="source_conn")
target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False
}

tables = ["базы_данных", "базы_данных_и_уровень_знаний_сотру", "инструменты", "инструменты_и_уровень_знаний_сотр",
          "образование_пользователей", "опыт_сотрудника_в_отраслях", "опыт_сотрудника_в_предметных_обла", "отрасли",
          "платформы", "платформы_и_уровень_знаний_сотруд", "предметная_область", "резюмедар", "сертификаты_пользователей",
          "сотрудники_дар", "среды_разработки", "среды_разработки_и_уровень_знаний_", "технологии", "технологии_и_уровень_знаний_сотру",
          "типы_систем", "типы_систем_и_уровень_знаний_сотру", "уровень_образования", "уровни_владения_ин", "уровни_знаний",
          "уровни_знаний_в_отрасли", "уровни_знаний_в_предметной_област", "фреймворки", "фреймворки_и_уровень_знаний_сотру",
          "языки", "языки_пользователей", "языки_программирования", "языки_программирования_и_уровень"]


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
        source_hook = PostgresHook(postgres_conn_id="source_conn")
        target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")

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
            scr_path = "/opt/airflow/scr/sql/create_scheme_dds.sql"
            with open(scr_path, "r") as file:
                sql_query = file.read()
            target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")
            target_hook.run(sql_query)
        except Exception as e:
            raise AirflowException(f"Failed to create DDS scheme: {e}")

    @task
    def preprocess_tables():
        try:
            dds_base_tables = ["базы_данных_и_уровень_знаний_сотру", "инструменты_и_уровень_знаний_сотр",
                               "образование_пользователей", "опыт_сотрудника_в_отраслях", "опыт_сотрудника_в_предметных_обла",
                               "платформы_и_уровень_знаний_сотруд", "сертификаты_пользователей", "среды_разработки_и_уровень_знаний_",
                               "технологии_и_уровень_знаний_сотру", "типы_систем_и_уровень_знаний_сотру",
                               "уровни_знаний_в_предметной_област", "фреймворки_и_уровень_знаний_сотру",
                               "языки_пользователей", "языки_программирования_и_уровень", "сотрудники_дар"]
            target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")
            for table in dds_base_tables:
                df = get_pd_df(table, target_hook, "andronov_ods")
                if table == "сертификаты_пользователей":
                    df = clean_table_df(df, drop_user_id=False)
                else:
                    df = clean_table_df(df, drop_user_id=True)
                if table == "сотрудники_дар":
                    df['подразделения'] = df['подразделения'].apply(lambda x: re.sub(r'[^a-zA-Zа-яА-Я\s\/]', '', str(x)))
                df.columns = [clean_column_names(col) for col in df.columns]
                df.to_csv(os.path.join(temp_dir, f"{table}.csv"), index=False)
        except Exception as e:
            raise AirflowException(f"Failed to preprocess tables: {e}")

    @task
    def preprocess_resume():
        try:
            target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")
            resume_df = get_pd_df("резюмедар", target_hook, "andronov_ods")
            columns_to_process = ["Образование", "Сертификаты/Курсы", "Языки", "Базыданных", "Инструменты",
                                  "Отрасли", "Платформы", "Предметныеобласти", "Средыразработки", "Типысистем",
                                  "Фреймворки", "Языкипрограммирования", "Технологии"]
            for column in columns_to_process:
                intermediate_df = create_intermediate_table(resume_df, column)
                column = column.lower().replace("/", "_")
                intermediate_df.to_csv(os.path.join(
                    temp_dir, f"резюме_{column}.csv"), index=False)
            resume_df = resume_df[["UserID", "ResumeID"]]
            resume_df.rename(
                columns={"UserID": "user_id", "ResumeID": "resumeid"}, inplace=True)
            resume_df.to_csv(os.path.join(
                temp_dir, "резюмедар.csv"), index=False)
        except Exception as e:
            raise AirflowException(f"Failed to preprocess resume: {e}")

    @task
    def insert_missing_ids():
        resume_df = pd.read_csv(os.path.join(temp_dir, "резюмедар.csv"))
        employees_df = pd.read_csv(
            os.path.join(temp_dir, "сотрудники_дар.csv"))

        resume_ids = set(resume_df["user_id"])
        employees_ids = set(employees_df["id"])

        missing_ids = resume_ids - employees_ids

        new_employees = pd.DataFrame({"id": list(missing_ids)})

        employees_df = pd.concat(
            [employees_df, new_employees], ignore_index=True)

        employees_df.to_csv(os.path.join(
            temp_dir, "сотрудники_дар.csv"), index=False)

    @task
    def load_tables_to_dds():
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping = json.load(f)

            target_hook = PostgresHook(postgres_conn_id='etl_db_4_conn')

            for table_name, csv_file in mapping.items():
                csv_path = os.path.join(temp_dir, csv_file)
                df = pd.read_csv(csv_path)

                truncate_query = f"TRUNCATE TABLE andronov_dds.{table_name} CASCADE;"
                target_hook.run(truncate_query)

                temp_table = f"temp_{table_name}"
                create_temp_table_query = f"CREATE TABLE IF NOT EXISTS {temp_table} (LIKE andronov_dds.{table_name} INCLUDING ALL);"
                target_hook.run(create_temp_table_query)

                engine = target_hook.get_sqlalchemy_engine()
                df.to_sql(name=temp_table, con=engine, if_exists='replace',
                          index=False, schema='andronov_dds')

                insert_query = f"INSERT INTO andronov_dds.{table_name} SELECT * FROM andronov_dds.{temp_table};"
                target_hook.run(insert_query)

                drop_temp_table_query = f"DROP TABLE andronov_dds.{temp_table};"
                target_hook.run(drop_temp_table_query)
        except Exception as e:
            raise AirflowException(f"Failed to load tables to DDS: {e}")

    @task
    def clear_temp_dir():
        try:
            for filename in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
        except Exception as e:
            raise AirflowException(f"Failed to clear the temp directory: {e}")

    # t1 = load_data_to_ods()
    t2 = create_dds_scheme()
    t3 = preprocess_tables()
    t4 = preprocess_resume()
    t5 = insert_missing_ids()
    t6 = load_tables_to_dds()
    t7 = clear_temp_dir()

    t2 >> t3 >> t4 >> t5 >> t6 >> t7


main_dag = taskflow()
