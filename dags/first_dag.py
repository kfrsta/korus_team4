import random
import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 7, 1),
    'retries': 2,
    'retry_delay': dt.timedelta(seconds=10),
}


def random_dice():
    val = random.randint(1, 6)
    if val % 2 != 0:
        raise ValueError(f'Odd {val}')


with DAG(dag_id='first_dag',  # уникальный id
         schedule_interval='@daily', # расписание запусков
         default_args=default_args) as dag:
    dice = PythonOperator(
        task_id='random_dice',
        python_callable=random_dice,
        dag=dag,
    )
