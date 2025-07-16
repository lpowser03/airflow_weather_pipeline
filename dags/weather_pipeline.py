# @Title: OpenWeatherMap API Airflow Pipeline
# @Author: Logan Powser
# @Date: 7/2/2025
# @Abstract: Utilize Airflow to regularly pull data from OpenWeatherMap API and store in PG DB

import os
import requests
import psycopg2
import airflow
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv('/mnt/c/Users/logan/source/repos/weather_prediction_airflow/.env')

# DAG configuration
default_args = {
    'owner': 'logan',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 16),
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# Create the DAG object
@dag(
    'weather_pipeline',
    default_args=default_args,
    description='Daily SF weather data collection',
    schedule_interval='@daily',  # Runs once per day
    catchup=False  # Don't run historical dates
)

def weather_pipeline():

    #extract task
    @task
    def get_weather_data():
        api_key = os.getenv("API_KEY")
        if not api_key:
           raise RuntimeError("API_KEY not found in environment variables.")
        lat, lon = 37.7749 , -122.4194
        base_url = f"http://api.openweathermap.org/data/2.5/weather?"
        complete_url = f"{base_url}lat={lat}&lon={lon}&appid={api_key}"
        response = requests.get(complete_url)
        return response.json()
    #transform task (turn kelvin into fahrenheit)
    @task
    def transform_weather_data(raw_data):
        kelvin = raw_data['main']['temp']
        fahrenheit = ((((kelvin - 273.15) * 9) / 5) + 32)

        transformed_data = {
            'temperature': round(fahrenheit, 2),
            'pressure': raw_data['main']['pressure'],
            'humidity': raw_data['main']['humidity'],
            'visibility': raw_data['visibility'],
            'wind_speed': raw_data['wind']['speed'],
            'weather_condition': raw_data['weather'][0]['main'],
        }

        return transformed_data
    #load data task (into Postgres)
    @task
    def insert_weather_data(clean_data):
        conn = psycopg2.connect(
            host=os.getenv("HOST"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        cur = conn.cursor()

        insert_sql = """
                INSERT INTO weather_data (collected_at, temperature, pressure, humidity, visibility, wind_speed, weather_condition) 
                VALUES (NOW(), %s, %s, %s, %s, %s, %s) 
                     """

        cur.execute(insert_sql, (
            clean_data['temperature'],
            clean_data['pressure'],
            clean_data['humidity'],
            clean_data['visibility'],
            clean_data['wind_speed'],
            clean_data['weather_condition']
        ))
        conn.commit()
        cur.close()
        conn.close()

    #task dependency setup
    raw_data = get_weather_data()
    clean_data = transform_weather_data(raw_data)
    insert_weather_data(clean_data)

#create dag
weather_dag = weather_pipeline()