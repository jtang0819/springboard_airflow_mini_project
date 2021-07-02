import yfinance as yf
# import pandas as pd
from datetime import timedelta, datetime, date
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
#DAG object

from airflow import DAG


def stonk(ticker):
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    df = yf.download(ticker, start=start_date, end=end_date, interval='1m')
    df.to_csv("{}_{}_data.csv".format(ticker, end_date), header=False)


def python_command():
	return(os.listdir('../tmp/data/2020/2020-09-24'))
	

default_args = {
    'owner': 'airflow',
    'email': ['jtang0819@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 9, 24)
}

with DAG(
    'marketvol',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='0 18 * * 1-5'
) as dag:

    t1 = PythonOperator(
		task_id='Tesla_Stonk_Data',
		python_callable=stonk,
		op_kwargs={'ticker': 'TSLA'}
    )

    t2 = PythonOperator(
        task_id='Apple_Stonk_Data',
        python_callable=stonk,
		op_kwargs={'ticker': 'AAPL'}
    )

    t3 = BashOperator(
        task_id='Tesla_Bash_Command',
        bash_command='''mv data.csv tmp/data/2020/2020-09-24'''
    )

    t4 = BashOperator(
        task_id='Apple_Bash_Command',
        bash_command='''mv data.csv tmp/dat/2020/2020-09-24'''
    )

    t5 = PythonOperator(
        task_id='Run_query_on_downloaded_data',
        python_callable=python_command
    )
t1.set_downstream(t3)
t2.set_downstream(t4)
[t3, t4] >> t5



