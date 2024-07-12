from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


@dag(
    dag_id='pipeline_4_andronov',
    default_args=default_args,
    description='main pileline dag',
    schedule_interval='0 4 * * *',
    start_date=datetime(2020, 1, 1),
    catchup=False
)
def ods_load_dag():

    def get_tables(source_hook):
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'source_data'
            AND table_type = 'BASE TABLE';
        """
        records = source_hook.get_records(query)
        return [record[0] for record in records]

    @task
    def load_data_to_ods():

        source_hook = PostgresHook(postgres_conn_id='source_conn')
        target_hook = PostgresHook(postgres_conn_id='etl_db_4_conn')

        tables = get_tables(source_hook)
        for table in tables:
            truncate_query = f"TRUNCATE TABLE andronov_ods.{table}"
            target_hook.run(truncate_query)
            select_query = f"SELECT * FROM source_data.{table}"
            records = source_hook.get_records(select_query)
            target_hook.insert_rows(f'andronov_ods.{table}', rows=records)

    load_data_ods_task = load_data_to_ods()


ods_load_dag()
