import pandas as pd
from tqdm import tqdm
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import yaml
import requests
from bs4 import BeautifulSoup

DATE_FORMAT = "%Y-%m-%d"
BASIC_PATH_TO_SAVE_FOLDER = '/usr/local/airflow/app/'


def get_date(**kwargs):
    with open(f"{BASIC_PATH_TO_SAVE_FOLDER}resources/input_date.yaml") as f:
        datetime = yaml.safe_load(f)["datetime"]
    kwargs['ti'].xcom_push(key='datetime', value=datetime)


def parse_weather(date: str, city: str) -> str:
    url = f"https://ua.sinoptik.ua/погода-{city}/{date}"
    
    response  = requests.get(url)
    soup = BeautifulSoup(response.text)
    tables = soup.find_all("table", {"class": "weatherDetails"})[0].find("tr", {"class": "temperature"}).find("td", {"class": "p4 bR"})
    
    return tables.text[:-1]


def get_weather_process(path_to_save: str, city: str, **kwargs) -> int:
    datetime_info = kwargs['ti'].xcom_pull(key='datetime', task_ids='get_date')

    start = datetime.strptime(datetime_info["from"], DATE_FORMAT)
    end = datetime.strptime(datetime_info["to"], DATE_FORMAT)

    date_generated = [start + timedelta(days=x) for x in range(0, (end - start).days)]
    

    result = []
    for date in tqdm(date_generated):
        day = date.strftime(DATE_FORMAT)    
        weather = parse_weather(day, city)
        result.append({"day": day, "weather": weather})

    df = pd.DataFrame(result)
    df.to_csv(f"{path_to_save}{city}-{start}-{end}.csv", index=False)

    return df.shape


default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 1, 26),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(days=1)
}

dag = DAG(
    'weather_process',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


get_date_task = PythonOperator(
    dag=dag,
    task_id='get_date',
    python_callable=get_date,
    provide_context=True,
)

vinnytsia_task = PythonOperator(
    dag=dag,
    task_id='vinnytsia_weather',
    python_callable=get_weather_process,
    provide_context=True,
    op_kwargs={
        'path_to_save': BASIC_PATH_TO_SAVE_FOLDER,
        'city': 'вінниця'
    },
)    
    
kiev_task = PythonOperator(
    dag=dag,
    task_id='kiev_weather',
    python_callable=get_weather_process,
    provide_context=True,
    op_kwargs={
        'path_to_save': BASIC_PATH_TO_SAVE_FOLDER,
        'city': 'київ'
    },
)    

run_task = BashOperator(
    dag=dag,
    task_id='run',
    bash_command='echo run pipeline',
)

end_task = BashOperator(
    dag=dag,
    task_id='end',
    bash_command='echo end',
)


run_task >> get_date_task >> [vinnytsia_task, kiev_task] >> end_task
