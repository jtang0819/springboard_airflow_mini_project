import glob
import os
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

root_dir = Path('logs')


def analyze_file():
    # - The total count of error entries from this file
    # - A list of error message details (the errors themself)
    # read file
    # iterate through line
    # add count of ERROR to count_error
    # if line has ERROR add error message to error_list
    count_error = 0
    error_list = []
    for filename in Path(root_dir).rglob('*.log'):
        with open(filename, 'r') as f:
            for line in f:
                if "ERROR" in line:
                    count_error += 1
                    error_list.append(line)
    return "Number of errors: " + str(count_error), "Here are the errors: ", error_list


default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 6, 26)
}


with DAG(
    'log_collector',
    default_args=default_args,
    description='A simple DAG',
    #schedule_interval='0 18 * * 1-5'
    schedule_interval='@once'
) as dag:

    t0 = PythonOperator(
        task_id='analyze_logs',
        python_callable=analyze_file

    )