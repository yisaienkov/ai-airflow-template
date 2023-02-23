import pandas as pd
from tqdm import tqdm
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import os
import re
from datetime import datetime, timedelta

PATH_TO_LOGS_FOLDER = 'resources/logs/'  # specify the path to the folder containing the log files
PATH_TO_SAVE_FOLDER = 'resources/parsed_logs'  # specify the path to the folder where the parsed logs will be saved

example_text = """
2023-01-19T07:18:41.929040+0000 INFO User [572256777] => Sent audio message
2023-01-19T07:18:49.298106+0000 INFO User [572256777] => Recognition text => Приклад збереження логів
2023-01-19T07:19:07.128429+0000 INFO User [572256777] => Asked for save data => Yes
2023-01-19T07:19:37.574467+0000 INFO User [572256777] => Input description => MDE_26
2023-01-19T07:19:37.574756+0000 INFO User [572256777] => Updated settings => {'language': 'uk-UA', 'description': 'Example'}
2023-01-19T07:19:37.667579+0000 INFO User [572256777] ~ Request-Status => <Response [200]>
"""


def write_to_file(path, text):
    with open(path, 'w') as f:
        f.write(text)
        

def preprocessing_log_files(path_to_logs_folder, path_to_save_folder):
    # Iterate through all log files in the folder
    log_data = []
    for filename in os.listdir(path_to_logs_folder):
        print(filename)
        if filename.endswith('.log'):
            log_file = path_to_logs_folder + filename
            with open(log_file, 'r') as f:
                logs = f.readlines()

            # Iterate through logs and extract relevant information
            for log in tqdm(logs):
                match = re.search(r'(.*)\s+(.*)\s+User\s+\[(.*)\]\s+=>\s+(.*)', log)
                if match:
                    timestamp, level, user_id, info = match.groups()
                    log_data.append([timestamp, level, user_id, info])

    # Create and save DataFrame
    pd.DataFrame(log_data, columns=['datetime', 'level', 'user_id', 'information'])\
    .sort_values("datetime")\
    .to_csv(f"{path_to_save_folder}/logs.csv", index=False)
    
    
default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 1, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'transform_data_dag',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
)

echo_task = BashOperator(
    task_id='run_echo',
    bash_command='echo Hello World',
    dag=dag,
    schedule_interval=timedelta(hours=1),
)

write_to_file_task = PythonOperator(
    task_id='write_to_file',
    python_callable=write_to_file,
    op_kwargs={'file_path': f"{PATH_TO_LOGS_FOLDER}tmp.log", 'example_text': example_text},
    dag=dag,
)

preprocessing_log_files_task = PythonOperator(
    task_id='preprocessing_log_files',
    python_callable=preprocessing_log_files,
    op_kwargs={'path_to_logs': PATH_TO_LOGS_FOLDER, 'path_to_save': PATH_TO_SAVE_FOLDER},
    dag=dag,
)

# Set the order of tasks
echo_task >> write_to_file_task >> preprocessing_log_files_task