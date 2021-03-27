import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from dashboard import clean_and_analyze_covid_data
from data_loading import get_JH_github_data

default_args = {
                'owner': 'Olga',
                'start_date': dt.datetime(2021, 3, 26),
                'retries': 1,
                'retry_delay': dt.timedelta(hours=1),
                }


with DAG('covid_dashboard_v1',
         default_args=default_args, 
         schedule_interval='0 * * * *',
         ) as dag:
    update_data = PythonOperator(task_id='update_data',
                                 python_callable=get_JH_github_data)
    
    
    update_data = PythonOperator(task_id='clean_and_analyze_covid_data',
                                 python_callable=clean_and_analyze_covid_data)
    
#     restart_server = PythonOperator(task_id='restart_server',
#                                  python_callable=restart_server)
    
    
