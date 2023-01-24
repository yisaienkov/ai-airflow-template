from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'me',
    'start_date': datetime(2024, 1, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
)

ls_task = BashOperator(
    task_id='run_ls',
    bash_command='ls',
    dag=dag,
    schedule_interval=timedelta(hours=1),
)

echo_task = BashOperator(
    task_id='run_echo',
    bash_command='echo Hello World',
    dag=dag,
    schedule_interval=timedelta(hours=1),
)

mkdir_task = BashOperator(
    task_id='run_mkdir',
    bash_command='mkdir test',
    dag=dag,
    schedule_interval=timedelta(hours=1),
)

get_global_path_task = BashOperator(
    task_id='get_global_path',
    bash_command='pwd',
    dag=dag,
    shedule_interval=timedelta(hours=1),
)


ls_task >> echo_task >> mkdir_task >> get_global_path_task >> ls_task
