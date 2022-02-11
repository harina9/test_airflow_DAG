import pandas as pd


from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime

pd.options.mode.chained_assignment = None

engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')


def read_table_with_avg_weather():
    script = "SELECT * FROM averg_temp"
    df = pd.read_sql(script, con=engine)

    return df


def get_aver_temp():
    script = """SELECT avg(minute_weather.temp_weather) as avg_temp FROM minute_weather WHERE 
    minute_weather.date_weather > (NOW() - INTERVAL '10 MINUTES') """
    df = pd.read_sql(script, con=engine)
    df['interval_date'] = pd.read_sql('SELECT NOW() FROM minute_weather;', con=engine)

    return df


full_table = read_table_with_avg_weather()

new_avg_temp = get_aver_temp()


def add_new_avg_temp_with_status(df_1, df_2):
    new_df = pd.concat([df_1, df_2], ignore_index=True)

    a = new_df['avg_temp'][-1:].reset_index(drop=True)
    b = new_df['avg_temp'][-2: -1].reset_index(drop=True)

    if a.item() > b.item():
        new_df['state'][-1:] = 'up'
    elif a.item() < b.item():
        new_df['state'][-1:] = 'down'
    else:
        new_df['state'][-1:] = 'same'

    return new_df


def write_to_db():
    raw_df = add_new_avg_temp_with_status(full_table, new_avg_temp)
    raw_df.to_sql('averg_temp', con=engine, if_exists='replace', index=False)


with DAG("new_dag", start_date=datetime(2022, 2, 9),
         schedule_interval='*/10 * * * *') as dag:
    get_weather = PythonOperator(
        task_id="write_to_db"
    )
