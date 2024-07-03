from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'test_dag',
    default_args=default_args,
    description='bim bim bam bam',
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
)


def sayhello():
    print(f"Hello World, {datetime.now()}")


load_data_to_ods_task = PythonOperator(
    task_id='test_task',
    python_callable=sayhello,
    dag=dag,
)