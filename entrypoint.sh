#!/bin/bash
set -e

# Initialize DB
airflow db init

# Create user (only if not exists, add a check if needed)
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --password admin \
    --email admin@example.com

# Install requirements
pip install -r /app/requirements.txt

# Start webserver and scheduler in background
airflow webserver & 
airflow scheduler
