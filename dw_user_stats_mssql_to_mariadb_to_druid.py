# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta, date
from airflow.utils.trigger_rule import TriggerRule

import urllib3

urllib3.disable_warnings()

# DAGs 의 성공/실패 정보를 Kafka API 서버에 전달하는 Custom Operator import
try:
    from KafkaRestApiCallOperator import KafkaRestApiCallOperator
except ImportError:
    from airflow.operators import KafkaRestApiCallOperator


sshHook_imply_3113 = SSHHook(
    ssh_conn_id='imply_3113'
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
    'dw_user_stats',
    default_args=default_args,
    description='B2C 회원 통계 데이터 -> MariaDB, Druid 적재',
    schedule_interval='00 12 * * *' # 오전 2:20
    # schedule_interval='52 07 * * *'
)


# [DW] 순수B2C 회원 통계
# MSSQL to MariaDB
command_t1 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_user_stats_b2c_info_mssql_to_mariadb.py {{params.DATE}}"

# MariaDB to Json
command_t2 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_user_stats_b2c_info_mariadb_to_json.py {{params.DATE}}"

command_t3 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_user_stats_b2c_latest_info_mariadb_to_json.py {{params.DATE}}"

# b2c_user Druid config setting
command_t4 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-user-stats-b2c-info.py "

# b2c_user Json to Druid
command_t5 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-user-stats-b2c-info/dw-user-stats-b2c-info.json "

# b2c_user_latest Druid config setting
command_t6 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-user-stats-b2c-latest-info.py "

# b2c_user_latest Json to Druid
command_t7 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-user-stats-b2c-latest-info/dw-user-stats-b2c-latest-info.json "



param_date = (datetime.utcnow() + timedelta(hours=+9)).strftime("%Y-%m-%d")
print(param_date)
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
    task_id='b2c_user_mssql_to_mariadb',
    command=command_t1,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=get_start_info,  # 성공 시 시작시간 정보 저장
    params={'DATE': param_date},
    dag=dag)

t2 = SSHOperator(
    task_id='b2c_user_mariadb_to_json',
    command=command_t2,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,
    params={'DATE': param_date},
    dag=dag)
    
t3 = SSHOperator(
    task_id='b2c_user_latest_mariadb_to_json',
    command=command_t3,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,
    params={'DATE': param_date},
    dag=dag)

t4 = SSHOperator(
    task_id='b2c_user_druid_setting',
    command=command_t4,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,
    dag=dag)

t5 = SSHOperator(
    task_id='b2c_user_json_to_druid',
    command=command_t5,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,
    dag=dag)

t6 = SSHOperator(
    task_id='b2c_user_latest_druid_setting',
    command=command_t6,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,
    dag=dag)

t7 = SSHOperator(
    task_id='b2c_user_latest_json_to_druid',
    command=command_t7,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,
    on_success_callback=result_msg,
    dag=dag)
    
t1.set_downstream(t2)
t2.set_downstream(t3)
t3.set_downstream(t4)
t4.set_downstream(t5)
t5.set_downstream(t6)
t6.set_downstream(t7)
