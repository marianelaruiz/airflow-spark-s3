#!/bin/bash

# Go to the airflow directory
cd airflow

# Set the AIRFLOW_HOME environment variable
export AIRFLOW_HOME=$(pwd)

# Set versions
export AIRFLOW_VERSION=2.9.0
export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Apache Airflow
pip install "apache-airflow>=${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Initialize the Airflow database
airflow db init
