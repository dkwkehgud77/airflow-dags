# -*- coding: utf-8 -*-


import airflow
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator

import urllib3

from datetime import timedelta

urllib3.disable_warnings()

# DAGs 의 성공/실패 정보를 Kafka API 서버에 전달하는 Custom Operator import
try:
    from KafkaRestApiCallOperator import KafkaRestApiCallOperator
except ImportError:
    from airflow.operators import KafkaRestApiCallOperator


sshHook_m2d2_5135 = SSHHook(
    ssh_conn_id='m2d2_5135'
)

# sshHook_gpu_80122 = SSHHook(
#     ssh_conn_id='gpu_80122',
# )

sshHook_curation_2321 = SSHHook(
    ssh_conn_id='curation_2321'
)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['sy.lee@hunet.co.kr'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# schedule_interval=timedelta(days=1)
# * * * * *
# 분 시 일 월 요일
dag = DAG(
    'curation_process_matching',
    default_args=default_args,
    description='특정 기업 키워드 베이스 큐레이션',
    # schedule_interval='01 15 * * *' # 12시 01분
    # changed time from 24:00 to 01:00 for test
    schedule_interval='01 16 * * *' # 01시 01분
    # schedule_interval='31 16 * * *'
)

command_t1 = "/home/hunetdb/anaconda3/envs/curation/bin/python " \
             "/home/hunetdb/src/script/curation/keyword_matching_init.py "



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


# t1, t2 and t3 are examples of tasks created by instantiating operators

t1 = SSHOperator(
    task_id='curation_process_matching',
    command=command_t1,
    ssh_hook=sshHook_curation_2321,
    on_success_callback=get_start_info,  # 성공 시 시작시간 정보 저장
    execution_timeout=timedelta(hours=1),  # 타 airflow 스케쥴 정상 동작을 위해서 시간 제한
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)


t2 = BashOperator(
    task_id='sleep1',
    depends_on_past=False,
    bash_command='sleep 5',
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=result_msg,
    dag=dag)

t1.set_downstream(t2)
