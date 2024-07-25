import numpy as np
import pandas as pd
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
import sys
sys.path.append('/opt/airflow/scr/')
from tasks import load_data_to_ods, create_dds_scheme, create_broken_schema, process_load_dds_measurments_tables, process_load_dds_extra_tables, process_load_dds_base_tables, create_dm_schema, load_data_dm

source_hook = PostgresHook(postgres_conn_id="source_conn")
target_hook = PostgresHook(postgres_conn_id="etl_db_4_conn")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False
}

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
    def load_data_to_ods_task():
        load_data_to_ods()

    @task
    def create_dds_scheme_task():
        create_dds_scheme()

    @task
    def create_broken_schema_task():
        create_broken_schema()

    @task
    def process_load_dds_measurments_tables_task():
        process_load_dds_measurments_tables()

    @task
    def process_load_dds_extra_tables_task():
        process_load_dds_extra_tables()

    @task
    def process_load_dds_base_tables_task():
        process_load_dds_base_tables()

    @task
    def create_dm_schema_task():
        create_dm_schema()

    @task
    def load_data_dm_task():
        load_data_dm()

    t1 = load_data_to_ods_task()
    t2 = create_dds_scheme_task()
    t3 = create_broken_schema_task()
    t4 = process_load_dds_measurments_tables_task()
    t5 = process_load_dds_extra_tables_task()
    t6 = process_load_dds_base_tables_task()
    t7 = create_dm_schema_task()
    t8 = load_data_dm_task()

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8


main_dag = taskflow()
