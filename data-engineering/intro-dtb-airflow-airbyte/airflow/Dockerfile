FROM apache/airflow:3.1.5

USER airflow

# Needed for:
# - DockerOperator (your DAG imports airflow.providers.docker...)
# - FAB auth manager so `airflow users create` exists in Airflow 3
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker \
    apache-airflow-providers-fab
