#!/bin/bash
set -e

# if [ -e "/opt/airflow/requirements.txt" ]; then
#   $(command python) pip install --upgrade pip
#   $(command -v pip) install --user -r requirements.txt
# fi

# If requirements.txt exists and is not empty
if [ -s "/requirements.txt" ]; then
  echo "Installing Python packages from /requirements.txt..."
  python -m pip install --upgrade pip
  pip install --user -r /requirements.txt
else
  echo "No requirements.txt found or it's empty, skipping installation."
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver