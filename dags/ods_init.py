from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


def copy_schema(source_conn_id, target_conn_id):
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)

    source_conn = source_hook.get_conn()
    target_conn = target_hook.get_conn()

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    target_cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS ods;
    """)
    target_conn.commit()

    source_cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'source_data';
    """)

    tables = source_cursor.fetchall()
    for table in tables:
        table_name = table[0]

        source_cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """)

        columns = source_cursor.fetchall()
        create_table_query = f"CREATE TABLE IF NOT EXISTS ods.{table_name} ("
        column_definitions = []
        for col in columns:
            column_name = col[0]
            data_type = col[1]
            column_name = f'"{column_name}"'
            column_definitions.append(f"{column_name} {data_type}")

        create_table_query += ", ".join(column_definitions)
        create_table_query += ");"

        target_cursor.execute(create_table_query)
        target_conn.commit()

    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()


with DAG(
    'ods_init',
    default_args=default_args,
    description='makes a copy of a source scheme into ods layer',
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False
) as dag:
    copy_schema_task = PythonOperator(
        task_id='copy_schema',
        python_callable=copy_schema,
        op_args=['source_conn', 'etl_db_4_conn']
    )

    copy_schema_task
