FROM apache/airflow:2.9.1

USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY requirements.txt /app/requirements.txt

USER airflow
ENTRYPOINT ["/entrypoint.sh"]
