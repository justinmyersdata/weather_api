import pandas as pd
import os

df = pd.read_csv('/opt/airflow/data/weather_2024_07_30.txt')

print(df.head())
