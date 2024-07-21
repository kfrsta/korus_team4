import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

source_conn_id = 'source_db'
target_conn_id = 'etl_db_4'
source_schema_name = 'dvyacheslav_ods'
target_schema_name = 'dvyacheslav_intermediate'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}

table_names = ['опыт_сотрудника_в_предметных_обла', 'опыт_сотрудника_в_отраслях', 'языки_пользователей',
               'технологии_и_уровень_знаний_сотру', 'платформы_и_уровень_знаний_сотруд',
               'инструменты_и_уровень_знаний_сотр', 'базы_данных_и_уровень_знаний_сотру',
               'среды_разработки_и_уровень_знаний_', 'фреймворки_и_уровень_знаний_сотру',
               'языки_программирования_и_уровень', 'типы_систем_и_уровень_знаний_сотру']


def create_intermediate_tables():
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    with target_hook.get_conn() as conn:
        with conn.cursor() as target_cur:
            for table in table_names:
                for schema in ['dvyacheslav_intermediate', 'dvyacheslav_broken_data']:
                    query = f"""SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table}' AND table_schema = '{source_schema_name}';"""

                    target_cur.execute(query)
                    data = target_cur.fetchall()

                    s = f"CREATE TABLE IF NOT EXISTS {schema}.{table} ("
                    for column_name, data_type in data:
                        if column_name not in ['активность', 'сорт.', 'дата_изм.']:
                            s += f'"{column_name}" {data_type}, '
                    create_table_query = s.rstrip(', ') + ');'

                    target_cur.execute(create_table_query)


with DAG(
        dag_id='create_intermediate_tables',
        description='Перенос промежуточных таблиц, без их содержимого (скорее всего этот даг будет удален)',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    create_schema = PostgresOperator(
        task_id='create_intermediate_and_broken_data_schema',
        sql='sql/create_intermediate_schema.sql',
        postgres_conn_id=target_conn_id,
    )

    create_intermediate_tables_in_schema = PythonOperator(
        task_id='create_intermediate_tables_in_schema',
        python_callable=create_intermediate_tables,
        dag=dag
    )

    create_schema >> create_intermediate_tables_in_schema
