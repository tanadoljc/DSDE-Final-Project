from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='external_data_pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['external_data']
) as dag:

    start_producer = BashOperator(
        task_id='start_kafka_producer',
        bash_command='python /opt/airflow/kafka/kafka_producer.py'
    )

    pm25_consumer = BashOperator(
        task_id='run_pm25_consumer',
        bash_command='python /opt/airflow/kafka/consumer_pm25.py'
    )

    rainfall_consumer = BashOperator(
        task_id='run_rainfall_consumer',
        bash_command='python /opt/airflow/kafka/consumer_rainfall.py'
    )

    # holiday_scraper = BashOperator(
    #     task_id='scrape_holidays',
    #     bash_command='python /app/holiday_scraper.py'
    # )

    start_producer >> [pm25_consumer, rainfall_consumer]