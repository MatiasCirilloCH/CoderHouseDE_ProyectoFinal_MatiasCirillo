from airflow.models import DAG, Variable

from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import smtplib
from email.mime.text import MIMEText
from email.header import Header
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
  
def send_task_fail(context):
    try:
        # Get failed task information
        task_instance = context['task_instance']
        task_id = task_instance.task_id
        dag_id = task_instance.dag_id
        execution_date = task_instance.execution_date
        log_url = context['task_instance'].log_url

        # Build email message
        email_from  = Variable.get('SMTP_EMAIL_FROM')
        email_to = Variable.get('SMTP_EMAIL_TO')
        subject=f"La tarea {task_id} del DAG {dag_id} ha fallado"
        body =  f"""
        La tarea {task_id} del DAG {dag_id} ha fallado.
        Fecha de ejecución: {execution_date}
        Puedes revisar los logs aquí: {log_url}
        """

        msg = MIMEText(body, 'plain', 'utf-8')
        msg['From'] = email_from
        msg['To'] = email_to
        msg['Subject'] = str(Header(subject, 'utf-8'))

        # Send email
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()#
        x.login(email_from, Variable.get('SMTP_PASSWORD'))
        x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'), msg.as_string())
        print('Exito')

    except Exception as exception:
        print(exception)
        print('Failure')
        raise exception

defaul_args = {
    "owner": "Matias Cirillo",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    'on_failure_callback': send_task_fail,
}

with DAG(
    dag_id="etl_weather",
    default_args=defaul_args,
    description="ETL de la tabla users",
    schedule="5,20,35,50 * * * *",
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

    create_table >> etl_task
