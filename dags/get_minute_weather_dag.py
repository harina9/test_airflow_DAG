import json
import pandas as pd
import urllib.request as req
import sqlalchemy

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from decouple import config


def get_current_weather() -> dict:
    """
    Gets temperature state in London.
    :return: dicts with jsons info.
    """
    api_key = config('api_key', default="")
    key = config('key', default="")
    url = f"http://api.weatherapi.com/v1/current.json?key={key}=London&aqi=no"
    units_of_measure = "metric"

    with req.urlopen(
            url.format(units_of_measure, api_key)
    ) as session:
        response = session.read().decode()
        data = json.loads(response)

    return data


def get_only_temp() -> pd.Dataframe:
    new_dict = get_current_weather()
    date = new_dict['location']['localtime']
    temp = new_dict['current']['temp_c']
    data = {'date_weather': [date], 'temp_weather': [temp]}
    new_df = pd.DataFrame(data)

    return new_df


engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')


def write_to_db():
    raw_df = get_only_temp()
    raw_df.to_sql('raw_weather', con=engine, if_exists='append', index=False)


with DAG("my_dag", start_date=datetime(2022, 2, 9),
         schedule_interval='*/1 * * * *') as dag:
    get_weather = PythonOperator(
        task_id="get_weather",
        python_callable=write_to_db
    )
