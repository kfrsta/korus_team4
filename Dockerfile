FROM apache/airflow:latest

COPY dags/ /opt/airflow/dags/
COPY logs/ /opt/airflow/logs/
COPY config/ /opt/airflow/config/
COPY plugins/ /opt/airflow/plugins/
COPY scr/ /opt/airflow/scr/
COPY table_config/ /opt/airflow/table_config/

RUN pip install pandas

ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
ENV AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
ENV AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
ENV AIRFLOW__CORE__FERNET_KEY=
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
ENV AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true

CMD ["webserver"]
