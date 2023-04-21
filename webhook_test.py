# -*- coding: utf-8 -*-
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

import WebhookCallOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'sla': timedelta(hours=2),
    'on_failure_callback': WebhookCallOperator.failed_callback,
    'start_date': datetime(2023, 3, 10, 14, 57, 20)
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'webhook_test',
    default_args=default_args,
    description='webhook_test',
    schedule_interval=timedelta(days=1),
)


def raise_exception():
    err_message = "Error Message"
    raise Exception(err_message)

# task
t2 = PythonOperator(task_id='callback_test', python_callable=raise_exception, dag=dag)
