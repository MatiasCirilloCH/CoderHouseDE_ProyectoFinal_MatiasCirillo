# Este es el DAG que orquesta el ETL de la tabla users

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta

import sys
sys.path.insert(0, '/opt/airflow/scripts')
from main import main

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS weather (
    country VARCHAR(50) NOT null distkey,
    location_name VARCHAR(50) NOT NULL,
    temperature FLOAT NOT NULL,
    wind_speed FLOAT NOT NULL,
    wind_direction VARCHAR(4) NOT NULL,
    pressure FLOAT NOT NULL,
    humidity INT NOT NULL,
    cloud INT NOT NULL,
    feels_like FLOAT NOT NULL,
    visibility FLOAT NOT NULL,
    last_updated DATETIME NOT NULL
) sortkey(temperature);
"""

defaul_args = {
    "owner": "Matias Cirillo",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_weather",
    default_args=defaul_args,
    description="ETL de la tabla users",
    schedule="1,16,31,46 * * * *",
    catchup=False,
) as dag:
    
    # Tareas
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    etl_task = PythonOperator(
        task_id="etl_task",
        python_callable=main,
        provide_context=True,
        dag=dag,
    )
    
    # ver path actual
    # bash_task = BashOperator(
    # task_id="bash_task",
    # bash_command='ls /opt/airflow/scripts', 
    # )

    create_table >> etl_task
