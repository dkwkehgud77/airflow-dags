# -*- coding: utf-8 -*-


import airflow
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator


import urllib3
import requests
import json

from datetime import timedelta

urllib3.disable_warnings()

# DAGs 의 성공/실패 정보를 Kafka API 서버에 전달하는 Custom Operator import
try:
    from KafkaRestApiCallOperator_test import KafkaRestApiCallOperator
except ImportError:
    from airflow.operators import KafkaRestApiCallOperator


sshHook_imitation_legacy_2323 = SSHHook(
    ssh_conn_id='imitation_legacy_2323'
)

sshHook_news_crawling_2930  = SSHHook(
    ssh_conn_id='news_crawling_2930'
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['sy.lee@hunet.co.kr'],
    'email_on_failure': False,
    'email_on_retry': False,
}


def fn_webhook_send(context):
    exception: str = str(context['exception']).split(':')[-1]
    dag_name: str = context['dag']
    task_name: str = str(context['task']).split(':')[-1][:-1]

    data = {
        "message": "## {} ##\n"
                   "오류 원인 : {}\n"
                   "{} 단계에서 실패 하였습니다".format(dag_name, exception, task_name),

        "targets": ["2021021"]
    }

    webhook_url = "https://h-support.hunet.name/api/webhook"
    requests.post(
        webhook_url, data=json.dumps(data),
        headers={'Content-Type': 'application/json'}
    )


# schedule_interval=timedelta(days=1)
# * * * * *
# 분 시 일 월 요일
dag = DAG(
    'sleep_task_test',
    default_args=default_args,
    description='Test Dag',
    schedule_interval='50 15 * * *' # 12시 01분
    # schedule_interval='31 16 * * *'
)

# command_t1 = "/home/hunetdb/anaconda3/envs/curation/bin/python " \
#              "/home/hunetdb/src/test/init.py "
command_t1 = "/home/hunetailab/anaconda3/envs/news_crawling/bin/python " \
             "/home/hunetailab/src/newscrawling/dag_test.py"
             # "/home/hunetailab/src/newscrawling/send_file_test.py "

# command_t2 = "/home/hunetdb/anaconda3/envs/curation/bin/python " \
#              "/home/hunetdb/src/testscript.py "

# 처음 시작되는 task의 시작 시간을 조회하여, xcom 영역에 저장한다.
def get_start_info(context):
    context['ti'].xcom_push(key="start_info", value={
        "dag_id": context['ti'].dag_id,
        "hostname": str(context['ti'].hostname),
        "start_time": context['ti'].start_date.strftime("%Y-%m-%dT%H:%M:%S")
    })


# 성공/실패 시 kafka.hunet.co.kr(10.140.90.51) Kafka Api 서버에 결과 값을 전달 한다.
def result_msg(context):
    kafka_msg_send = KafkaRestApiCallOperator(task_id="kafka_restapi_msg_send", endpoint="/kafka_api")
    kafka_msg_send.execute(context)

def test_callback(context: dict):
    print(f'context type: {type(context)}')
    for k, v in context.items():
        print(f"key: {k}")
        print(f"value: {v}")

t1 = SSHOperator(
    task_id='init_test',
    command=command_t1,
    ssh_hook=sshHook_news_crawling_2930,
    execution_timeout=timedelta(hours=5),  # 타 airflow 스케쥴 정상 동작을 위해서 시간 제한
    on_success_callback=test_callback,
    on_failure_callback=fn_webhook_send,  # 실패 시 정보전달
    dag=dag)

# t2 = BashOperator(
#     task_id='sleep_test',
#     depends_on_past=False,
#     bash_command = 'sleep 60m',
#     on_failure_callback=result_msg,  # 실패 시 정보전달
#     dag=dag)
#
# t3 = SSHOperator(
#     task_id='second_init_test',
#     command=command_t2,
#     ssh_hook=sshHook_curation_2321,
#     on_failure_callback=result_msg,  # 실패 시 정보전달
#     on_success_callback=result_msg,
#     dag=dag)


# t1.set_downstream(t2)
# t2.set_downstream(t3)
