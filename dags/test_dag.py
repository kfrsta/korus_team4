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
    dag_id='test_dag',
    default_args=default_args,
    description='get a list of tables from source db',
    schedule_interval="@once",
    start_date=datetime(2020, 1, 1),
    catchup=False,
)
def test_dag():
    @task
    def get_data():
        source_hook = PostgresHook(postgres_conn_id='source_conn')
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'source_data'
            AND table_type = 'BASE TABLE';
        """
        records = source_hook.get_records(query)
        print([record[0] for record in records])
    get_data()


test_dag()