import psycopg2
import sys

import datetime
import pytz
from airflow.hooks.postgres_hook import PostgresHook


date = datetime.datetime.now(pytz.timezone('US/Central')).strftime('%Y_%m_%d')

#First Command is table you want to run, second command is the file from which it is coming
def copy_expert_csv(file):
    hook = PostgresHook("postgres_localhost")
    with hook.get_conn() as connection:
        hook.copy_expert(
            f"""
        COPY weather.daily_weather FROM stdin WITH CSV HEADER DELIMITER as ','
        """,
            f"{file}",
        )
        connection.commit()

if __name__=='__main__':
    copy_expert_csv(f'/opt/airflow/data/weather_{date}.csv')