import datetime as dt

from airflow import DAG
from airflow.operators.generic_transfer import GenericTransfer

source_conn_id = 'source_db'
target_conn_id = 'etl_db_4'
source_schema_name = 'source_data'
target_schema_name = 'dvyacheslav_ods'

default_args = {
    'owner': 'Vyacheslav',
    'start_date': dt.datetime(2024, 7, 1),
}

with DAG(dag_id='generic_transfer',
         schedule_interval='@once',
         default_args=default_args) as dag:
    test_sql = GenericTransfer(
        task_id='testing',
        sql=f"SELECT * FROM {source_schema_name}.базы_данных ",
        source_conn_id=source_conn_id,
        destination_conn_id=target_conn_id,
        destination_table=f"{target_schema_name}.базы_данных",
        preoperator=f"TRUNCATE TABLE {target_schema_name}.базы_данных",
        dag=dag
    )
