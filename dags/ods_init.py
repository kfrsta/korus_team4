from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


@dag(
    dag_id='ods_init',
    default_args=default_args,
    description='makes a copy of a source scheme into ods layer',
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False
)
def copy_schema_dag():

    @task
    def copy_schema():
        source_hook = PostgresHook('source_conn')
        target_hook = PostgresHook('etl_db_4_conn')

        source_conn = source_hook.get_conn()
        target_conn = target_hook.get_conn()

        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()

        target_cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS andronov_ods;
        """)
        target_conn.commit()

        tables = ['базы_данных', 'базы_данных_и_уровень_знаний_сотру', 'инструменты', 'инструменты_и_уровень_знаний_сотр', 'образование_пользователей', 'опыт_сотрудника_в_отраслях', 'опыт_сотрудника_в_предметных_обла', 'отрасли', 'платформы', 'платформы_и_уровень_знаний_сотруд', 'предметная_область', 'резюмедар', 'сертификаты_пользователей', 'сотрудники_дар', 'среды_разработки',
                  'среды_разработки_и_уровень_знаний_', 'технологии', 'технологии_и_уровень_знаний_сотру', 'типы_систем', 'типы_систем_и_уровень_знаний_сотру', 'уровень_образования', 'уровни_владения_ин', 'уровни_знаний', 'уровни_знаний_в_отрасли', 'уровни_знаний_в_предметной_област', 'фреймворки', 'фреймворки_и_уровень_знаний_сотру', 'языки', 'языки_пользователей', 'языки_программирования', 'языки_программирования_и_уровень']

        for table_name in tables:
            source_cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s;
            """, (table_name,))

            columns = source_cursor.fetchall()
            column_definitions = [
                f'"{column_name}" {data_type}' for column_name, data_type in columns]

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS andronov_ods.{table_name} (
                    {", ".join(column_definitions)}
                );
            """

            target_cursor.execute(create_table_query)
            target_conn.commit()

        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()

    copy_schema_task = copy_schema()


init_dag = copy_schema_dag()
