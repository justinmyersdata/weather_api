#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='weather_dag',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60)
) as dag:

    end_task = DummyOperator(
        task_id='end_task',
    )

    create_schema_database = BashOperator(
        task_id='create_schema_database',
        bash_command='python /opt/airflow/scripts/postgres_connect.py',
    )   

    # [START howto_operator_bash]
    get_weather_data = BashOperator(
        task_id='weather_ingest',
        bash_command='python /opt/airflow/scripts/weather_api.py',
    )
    # [END howto_operator_bash]

    import_csv = BashOperator(
        task_id='import_csv',
        bash_command='python /opt/airflow/scripts/copy_to_postgres.py',
    )



    create_schema_database >> get_weather_data >> import_csv >> end_task

if __name__ == "__main__":
    dag.cli()