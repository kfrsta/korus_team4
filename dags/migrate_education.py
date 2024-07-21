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

table = 'образование_пользователей'
table_attribute = {'образование_пользователей': ['уровень_образование']}


def load_and_process_data():
    hook = PostgresHook(postgres_conn_id=target_conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as connection:
        df = pd.read_sql_table(table_name=table, con=connection, schema=source_schema_name)
        df = df.drop(columns=['активность', 'сорт.', 'дата_изм.'])  # так лучше не делать, но пока так
        # поиск строк с пропусками по столбцу 'год_окончания'
        df_with_missing = df[(df['год_окончания'] == 0) | (df['год_окончания'] > 2024) | (df['год_окончания'].isnull())]
        df_without_missing = df[
            (df['год_окончания'] != 0) & (df['год_окончания'] <= 2024) & ~(df['год_окончания'].isnull())]
        # Сохранение DataFrame с пропусками в отдельную схему в базе данных
        df_with_missing.to_sql(name=table, con=connection,
                               schema=broken_data_schema_name, if_exists='replace',
                               index=False)
        # Сохранение DataFrame без пропусков в другую схему в базе данных и предварительная обработка
        for column in table_attribute[table]:
            df_without_missing[column] = df_without_missing[column].str.extract(r'\[(\d+)\]').astype(int)
        df_without_missing.to_sql(name=table, con=connection,
                                  schema=target_schema_name, dtype={'user_id': Integer},
                                  if_exists='replace',
                                  index=False)


with DAG(
        dag_id='migrate_education',
        default_args=default_args,
        description='''
        DAG для передачи обработанных данных из таблицы образование_пользователей
        в схемы broken_data и intermediate''',
        schedule_interval=None,
) as dag:
    migrate_education = PythonOperator(
        task_id='migrate_education',
        python_callable=load_and_process_data,
        dag=dag,
    )
