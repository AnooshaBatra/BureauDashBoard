from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from moduleetl.extract import *
from moduleetl.transform import *
from moduleetl.load import *


def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG('api_data_to_all_states', description='All states DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 5, 22), catchup=False)


# this task will get data from api, I am not sending any 
# parameters on `python_callback` it will use the default parameters. 
get_data_from_api_operator = PythonOperator(
    task_id='get_data_from_api', 
    python_callable=get_data_from_api, 
    dag=dag)

transform_data_operator = PythonOperator(
    task_id='transform_data', 
    python_callable=transform_data, 
    dag=dag)

# this task will upload data into the google sheet.
upload_data_into_sheet_operator = PythonOperator(
    task_id='upload_data_into_sheet_operator', 
   python_callable=upload_data_into_sheet, 
   dag=dag)


get_data_from_api_operator >> transform_data_operator >> upload_data_into_sheet_operator