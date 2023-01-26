import pandas as pd
from tqdm import tqdm
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import requests
from bs4 import BeautifulSoup

DATE_FORMAT = "%Y-%m-%d"
DATETIME_TO = "2023-01-26"
DATETIME_FROM = "2023-01-16"
BASIC_PATH_TO_SAVE_FOLDER = '/usr/local/airflow/app/'


def parse_weather(date: str, city: str) -> str:
    url = f"https://ua.sinoptik.ua/погода-{city}/{date}"
    
    response  = requests.get(url)
    soup = BeautifulSoup(response.text)
    tables = soup.find_all("table", {"class": "weatherDetails"})[0].find("tr", {"class": "temperature"}).find("td", {"class": "p4 bR"})
    
    return tables.text[:-1]


def get_weather_process(start: str, end: str, path_to_save: str, city: str) -> int:
    start = datetime.strptime(DATETIME_FROM, DATE_FORMAT)
    end = datetime.strptime(DATETIME_TO, DATE_FORMAT)

    date_generated = [start + timedelta(days=x) for x in range(0, (end - start).days)]

    result = []
    for date in tqdm(date_generated):
        day = date.strftime(DATE_FORMAT)    
        weather = parse_weather(day, city)
        result.append({"day": day, "weather": weather})
        # print(day, weather)

    df = pd.DataFrame(result)
    df.to_csv(f"{path_to_save}{city}-{start}-{end}.csv", index=False)

    print(df.shape)
    return 200


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


vinnytsia_task = PythonOperator(
    dag=dag,
    task_id='vinnytsia_weather',
    python_callable=get_weather_process,
    op_kwargs={
        'start': DATETIME_FROM,
        'end': DATETIME_TO,
        'path_to_save': BASIC_PATH_TO_SAVE_FOLDER,
        'city': 'вінниця'
    },
)    
    
kiev_task = PythonOperator(
    dag=dag,
    task_id='kiev_weather',
    python_callable=get_weather_process,
    op_kwargs={
        'start': DATETIME_FROM,
        'end': DATETIME_TO,
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


run_task >> [vinnytsia_task, kiev_task] >> end_task
