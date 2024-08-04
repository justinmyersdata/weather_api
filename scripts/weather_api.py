import pandas as pd
import requests
import datetime
import os
import pytz
from env import api_key
print(os.getcwd())

date = datetime.datetime.now(pytz.timezone('US/Central')).strftime('%Y_%m_%d')

zip_codes = [33433,32164,75206,90024]

zip_code_dfs = []
for zip_code in zip_codes:

    content = requests.get(f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={zip_code}&aqi=no')
    print(content.json())

    data = content.json()["current"]
    location = content.json()["location"]

    del data['condition']



    df_current = pd.DataFrame(data, index=[0])
    df_location = pd.DataFrame(location, index=[0])

    df = pd.concat([df_location,df_current],axis=1)

    zip_code_dfs.append(df)


final_df = pd.concat(zip_code_dfs,axis=0)

final_df.to_csv(f'/opt/airflow/data/weather_{date}.csv',index=False,mode='w')

