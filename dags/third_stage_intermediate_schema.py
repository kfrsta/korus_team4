import datetime as dt

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Integer

target_conn_id = 'etl_db_4'
source_schema_name = 'dvyacheslav_ods'
target_schema_name = 'dvyacheslav_intermediate'
broken_data_schema_name = 'dvyacheslav_broken_data'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}

table_names = [
    'опыт_сотрудника_в_предметных_обла', 'опыт_сотрудника_в_отраслях', 'языки_пользователей',
    'технологии_и_уровень_знаний_сотру', 'платформы_и_уровень_знаний_сотруд',
    'инструменты_и_уровень_знаний_сотр', 'базы_данных_и_уровень_знаний_сотру',
    'среды_разработки_и_уровень_знаний_', 'фреймворки_и_уровень_знаний_сотру',
    'языки_программирования_и_уровень', 'типы_систем_и_уровень_знаний_сотру',
    'сертификаты_пользователей'
]

table_attributes = {
    'опыт_сотрудника_в_предметных_обла': ['предментые_области', 'уровень_знаний_в_предметной_облас'],
    'опыт_сотрудника_в_отраслях': ['отрасли', 'уровень_знаний_в_отрасли'],
    'языки_пользователей': ['язык', 'уровень_знаний_ин._языка'],
    'технологии_и_уровень_знаний_сотру': ['технологии', 'уровень_знаний'],
    'платформы_и_уровень_знаний_сотруд': ['платформы', 'уровень_знаний'],
    'инструменты_и_уровень_знаний_сотр': ['инструменты', 'уровень_знаний'],
    'базы_данных_и_уровень_знаний_сотру': ['базы_данных', 'уровень_знаний'],
    'среды_разработки_и_уровень_знаний_': ['среды_разработки', 'уровень_знаний'],
    'фреймворки_и_уровень_знаний_сотру': ['фреймворки', 'уровень_знаний'],
    'языки_программирования_и_уровень': ['языки_программирования', 'уровень_знаний'],
    'типы_систем_и_уровень_знаний_сотру': ['типы_систем', 'уровень_знаний']
}


def load_and_process_data():
    hook = PostgresHook(postgres_conn_id=target_conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as connection:
        for table in table_names:
            df = pd.read_sql_table(table_name=table, con=connection, schema=source_schema_name)
            df = df.drop(columns=['активность', 'сорт.', 'дата_изм.'])

            # поиск строк с пропусками по любому столбцу
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
            if table in table_attributes:
                for column in table_attributes[table]:
                    df_without_missing[column] = df_without_missing[column].str.extract(r'\[(\d+)\]').astype(int)
            df_without_missing.to_sql(name=table, con=connection,
                                      schema=target_schema_name, dtype={'user_id': Integer},
                                      if_exists='replace',
                                      index=False)


with DAG(
        dag_id='pandas_processing_dag',
        default_args=default_args,
        description='Обработка и перенос данных из промежуточных таблиц',
        schedule_interval=None,
) as dag:
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=load_and_process_data,
        dag=dag,
    )
