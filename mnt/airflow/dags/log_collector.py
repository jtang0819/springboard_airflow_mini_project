from pathlib import Path
from airflow.operators.bash import BashOperator

rootdir = Path('opt/airflow/logs/').rglob('*.log')


def analyze_file(file):
    # TODO add function to return-
    # - The total count of error entries from this file
    # - A list of error message details (the errors themself)
    # read file
    # iterate through line
    # add count of ERROR to count_error
    # if line has ERROR add error message to error_list
    count_error = 0
    error_list = []
	file_list = [f for f in rootdir..glob('**/*' if f.is_file())]
	
    return count_error, error_list



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

    t0 = BashOperator(
        task_id='make_tmp',
        bash_command=commands_t0

    )