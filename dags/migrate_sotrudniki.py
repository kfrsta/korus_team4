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

table = 'сотрудники_дар'


def migrate_table_sotrudniki():
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:

            for schema in ['dvyacheslav_intermediate', 'dvyacheslav_broken_data']:
                query = f"""SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table}' AND table_schema = '{source_schema_name}';"""

                target_cur.execute(query)
                data = target_cur.fetchall()

                s = f"CREATE TABLE IF NOT EXISTS {schema}.{table} ("
                for column_name, data_type in data:
                    if column_name == "id":
                        s += f'"user_id" {data_type}, '
                    else:
                        s += f'"{column_name}" {data_type}, '
                create_table_query = s.rstrip(', ') + ');'

                target_cur.execute(create_table_query)


def migrate_information_sotrudniki():
    hook = PostgresHook(postgres_conn_id=target_conn_id)
    engine =  hook.get_sqlalchemy_engine()
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


with DAG(
        dag_id='migrate_sotrudniki',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    extract_and_insert_table = PythonOperator(
        task_id='migrate_table_sotrudniki',
        python_callable=migrate_table_sotrudniki,
        dag=dag
    )

    process_data_task = PythonOperator(
        task_id='migrate_information_sotrudniki',
        python_callable=migrate_information_sotrudniki,
        dag=dag,
    )

    extract_and_insert_table >> process_data_task
