import yfinance as yf
# import pandas as pd
from datetime import timedelta, datetime, date
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
# DAG object

from airflow import DAG


def stonk(ticker):
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    df = yf.download(ticker, start=start_date, end=end_date, interval='1m')
    df.to_csv("{}_data.csv".format(ticker), header=False)
    return os.getcwd(), os.listdir()


def python_command():
    return os.listdir('tmp/data/2020/2020-09-24')
    #return os.getcwd()


default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 6, 26)
}

commands_t0 = """
cd ~;
mkdir -p tmp/data/2020/2020-09-24;
pwd;
ls;
"""

commands_tsla = """
cd ~;
ls;
mv TSLA_data.csv tmp/data/2020/2020-09-24;
ls"""

commands_aapl = """
cd ~;
ls;
mv AAPL_data.csv tmp/data/2020/2020-09-24;
"""

with DAG(
    'marketvol',
    default_args=default_args,
    description='A simple DAG',
    #schedule_interval='0 18 * * 1-5'
    schedule_interval='@once'
) as dag:

    t0 = BashOperator(
        task_id='make_tmp',
        bash_command=commands_t0

    )

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
        bash_command=commands_tsla
        # bash_command='''mv TSLA_data.csv tmp/data/2020/2020-09-24'''
    )

    t4 = BashOperator(
        task_id='Apple_Bash_Command',
        bash_command=commands_aapl
        # bash_command='''mv AAPL_data.csv tmp/dat/2020/2020-09-24'''
    )

    t5 = PythonOperator(
        task_id='Run_query_on_downloaded_data',
        python_callable=python_command
    )

t1.set_downstream(t3)
t2.set_downstream(t4)
t3.set_downstream(t5)
t4.set_downstream(t5)
t0 >> [t1, t2]
