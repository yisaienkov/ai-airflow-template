from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


BASIC_PATH_TO_SAVE_FOLDER = '/usr/local/airflow/app/'


default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 1, 26),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(days=1)
}

dag = DAG(
    'my_parallel_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

run_task = BashOperator(
    dag=dag,
    task_id='run',
    bash_command='echo run pipeline',
)

task_start = BashOperator(
    dag=dag,
    task_id='start',
    bash_command='date'
)

echo_task = BashOperator(
    dag=dag,
    task_id='echo',
    bash_command='echo Hello World',
)

ls_task = BashOperator(
    dag=dag,
    task_id='ls',
    bash_command=f'ls {BASIC_PATH_TO_SAVE_FOLDER}',
)

def write_to_file(path_to_file, text):
    with open(path_to_file, 'w') as f:
        f.write(text)

demo_save_task = PythonOperator(
    dag=dag,
    task_id='save2file',
    python_callable=write_to_file,
    op_kwargs={'path_to_file': f"{BASIC_PATH_TO_SAVE_FOLDER}/test.txt", 'text': "Hello World"},
)    
    

get_object_task = BashOperator(
    dag=dag,
    task_id='get_object',
    bash_command=f'ls {BASIC_PATH_TO_SAVE_FOLDER}',
)

task_end = BashOperator(
    dag=dag,
    task_id='end',
    bash_command='echo end',
)    

run_task >> [task_start, task_start, echo_task, ls_task] >> task_end >> [demo_save_task, get_object_task]
