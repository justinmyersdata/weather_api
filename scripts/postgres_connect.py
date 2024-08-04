import psycopg2
from env import postgres_host,postgres_port,postgres_user,postgres_database,postgres_password


connection_string = f"host='{postgres_host}' port='{postgres_port}' dbname='{postgres_database}' user='{postgres_user}' password='{postgres_password}'"


with psycopg2.connect(connection_string) as conn:
    cursor = conn.cursor()

    with open('/opt/airflow/scripts/sql/weather_schema.sql','r') as weather_schema:
        query = weather_schema.read()
        print(query)
        cursor.execute(query)


    with open('/opt/airflow/scripts/sql/weather_table.sql','r') as weather_table:
        query = weather_table.read()
        print(query)
        cursor.execute(query)
    
    cursor.close()

