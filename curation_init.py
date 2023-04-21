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

# sshHook_gpu_70121 = SSHHook(
#     ssh_conn_id='gpu_70121',
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
    'curation_service_init',
    default_args=default_args,
    description='전체 기업 큐레이션',
    schedule_interval='01 15 * * *' # 12시 01분
    # schedule_interval='01 15 * * *' # 01시 01분
    # schedule_interval='31 16 * * *'
)

command_t1 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/Intergrate_Curation_get_mssql_to_s3_customized.py "

command_t2 = "/home/hunetdb/anaconda3/envs/curation/bin/python " \
             "/home/hunetdb/src/script/curation/Intergrate_Curation_Specific_training.py "

command_t3 = "/home/hunetdb/anaconda3/envs/curation/bin/python " \
             "/home/hunetdb/src/script/curation/Intergrate_Curation_Specific_db_insert.py "

command_t4 = "/home/hunetdb/anaconda3/envs/curation/bin/python " \
             "/home/hunetdb/src/script/curation/delete_data.py"

command_t5 = "/home/hunetdb/anaconda3/envs/curation/bin/python " \
             "/home/hunetdb/src/script/curation/delete_s3.py"


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

t1 = SSHOperator(
    task_id='Curation_MSSQL_to_S3_Customized',
    command=command_t1,
    ssh_hook=sshHook_m2d2_5135,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=get_start_info,  # 성공 시 시작시간 정보 저장
    dag=dag)

t2 = BashOperator(
    task_id='sleep_60_mins',
    depends_on_past=False,
    bash_command = 'sleep 60m',
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)

t3 = SSHOperator(
    task_id='Curation_Customized_Training',
    command=command_t2,
    ssh_hook=sshHook_curation_2321,
    execution_timeout=timedelta(hours=4),  # 타 airflow 스케쥴 정상 동작을 위해서 시간 제한 #2022.12.14 동작시간 조정 2->4
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)

t4 = SSHOperator(
    task_id='Curation_Customized_DB_Insert',
    command=command_t3,
    ssh_hook=sshHook_curation_2321,
    execution_timeout=timedelta(hours=4),  # 타 airflow 스케쥴 정상 동작을 위해서 시간 제한
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)

t5 = SSHOperator(
    task_id='Delete_Curation_data',
    command=command_t4,
    ssh_hook=sshHook_curation_2321,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)

t6 = SSHOperator(
    task_id='Delete_S3_data',
    command=command_t5,
    ssh_hook=sshHook_curation_2321,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=result_msg,
    dag=dag)

# t1, t2 and t3 are examples of tasks created by instantiating operators
# t1 = SSHOperator(
#     task_id='Curation_MSSQL_to_S3_Customized',
#     command=command_t1,
#     ssh_hook=sshHook_m2d2_5135,
#     on_failure_callback=result_msg,  # 실패 시 정보전달
#     on_success_callback=get_start_info,  # 성공 시 시작시간 정보 저장
#     dag=dag)
#
#
# t2 = SSHOperator(
#     task_id='Curation_Customized_Training',
#     command=command_t2,
#     ssh_hook=sshHook_curation_2321,
#     execution_timeout=timedelta(hours=2),  # 타 airflow 스케쥴 정상 동작을 위해서 시간 제한
#     on_failure_callback=result_msg,  # 실패 시 정보전달
#     dag=dag)
#
# t3 = SSHOperator(
#     task_id='Curation_Customized_DB_Insert',
#     command=command_t3,
#     ssh_hook=sshHook_curation_2321,
#     execution_timeout=timedelta(hours=4),  # 타 airflow 스케쥴 정상 동작을 위해서 시간 제한
#     on_failure_callback=result_msg,  # 실패 시 정보전달
#     dag=dag)
#
# t4 = SSHOperator(
#     task_id='Delete_Curation_data',
#     command=command_t4,
#     ssh_hook=sshHook_curation_2321,
#     on_failure_callback=result_msg,  # 실패 시 정보전달
#     dag=dag)
#
# t5 = SSHOperator(
#     task_id='Delete_S3_data',
#     command=command_t5,
#     ssh_hook=sshHook_curation_2321,
#     on_failure_callback=result_msg,  # 실패 시 정보전달
#     on_success_callback=result_msg,
#     dag=dag)

t1.set_downstream(t2)
t2.set_downstream(t3)
t3.set_downstream(t4)
t4.set_downstream(t5)
t5.set_downstream(t6)
