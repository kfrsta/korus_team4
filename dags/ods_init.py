from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.exceptions import AirflowException

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
def init_dag():

    @task
    def create_ods_schema():
        target_hook = PostgresHook('etl_db_4_conn')
        try:
            scr_path = "/opt/airflow/scr/sql/create_ods_schema.sql"
            with open(scr_path, "r") as file:
                sql_query = file.read()
            target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")
            target_hook.run(sql_query)
        except Exception as e:
            raise AirflowException(f"Failed to load_data schema: {e}")
        
    t1 = create_ods_schema()


init_dag = init_dag()
