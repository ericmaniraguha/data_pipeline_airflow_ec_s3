from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
# Import the ETL function from your module
from etl_pipeline import run_twitter_etl

# Default arguments for the DAG
default_args = {
    'owner': 'airflow', # identify who wrote the dag as you are working a team.
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),  # Set to a past date for testing
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='A DAG to extract tweets from Paul Kagame',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
    tags=['twitter', 'etl'],
)

# Task to run Twitter ETL
run_twitter_etl_task = PythonOperator(
    task_id='complete_twitter_etl',
    # This is the function that will be called to run the ETL process
    python_callable=run_twitter_etl,
    dag=dag,
)

# Set task dependencies (if there were multiple tasks)
# For now, there's only one task