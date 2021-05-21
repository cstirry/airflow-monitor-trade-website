# !/usr/bin/env python

"""
### Airflow pipeline to parse the trade action list website
for new updates by date and send an email with a daily update
"""

import airflow
from airflow import DAG
from datetime import datetime, timedelta
import os
import papermill as pm
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator


DAG_ID = "trade_list_daily_monitoring"

DT = datetime.today().strftime("%Y-%m-%d")
LOCAL_AIRFLOW_PATH = "/PATH/airflow/dags"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

def execute_python_notebook_task(**context):
    notebook_path = context['notebook_path']
    output_path = context['output_path']
    out_dir = os.path.dirname(output_path)
    statement_parameters = context['statement_parameters'] if 'statement_parameters' in context else None

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if callable(statement_parameters):
        statement_parameters = statement_parameters(context)

    pm.execute_notebook(
        notebook_path,
        output_path,
        parameters=statement_parameters
    )


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    dag.doc_md = __doc__

    execute_notebook = PythonOperator(
        task_id='execute_notebook',
        python_callable=execute_python_notebook_task,
        op_kwargs={
            "notebook_path": "/PATH/airflow/dags/input_notebook.ipynb",
            'output_path': "/tmp/output_notebook.ipynb",
            'statement_parameters': {
                'dt_0': DT
                }
            }
        )

    convert_notebook = BashOperator(
        task_id='convert_notebook',
        bash_command= 'jupyter nbconvert --to pdf --TemplateExporter.exclude_input=True {{params.notebook_path}}',
        params={
            'notebook_path': "/tmp/output_notebook.ipynb",
                }
        )

    email_file = "/tmp/output_notebook.pdf"
    email_notebook_attachment = EmailOperator(
        to=['EMAIL'],
        task_id='email_notebook_attachment',
        subject= 'Trade Action List Monitoring as of_'+DT,
        html_content='See PDF attachment',
        files=[email_file],
    )

    # dependencies
    execute_notebook >> convert_notebook >> email_notebook_attachment