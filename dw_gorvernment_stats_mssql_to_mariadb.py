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
    'dw_government',
    default_args=default_args,
    description='정부지원금 데이터 -> MariaDB, Druid 적재',
    schedule_interval='50 17 * * *' # 오전 2:40
    # schedule_interval='52 07 * * *'
)

# [DW] 학습

# MSSQL -> MariaDB
command_t1 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_gorvernment_stats_mssql_to_mariadb.py"

# MariaDB -> Json
command_t2 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_druid_government_stats_mariadb_to_json.py {{params.DATE}}"


# Delete datasource
command_t3 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/delete_datasource_government_stats.py "

# Change Config
# Json -> Druid
command_t4 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-government-stats.py "
command_t5 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-government-stats/dw-government-stats.json "
#
#command_t7 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-study-info2.py "
#command_t8 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-study-info/dw-study-info.json "



# # [DW] Old file delete
# command_t14 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
#               "/home/hunetdb/anaconda3/envs/dbadmin/script/old_file_delete.py"

param_date = (datetime.utcnow()).strftime("%Y-%m-%d")
param_date2 = '2019-01-01'

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
    task_id='Government_mssql_to_mariadb',
    command=command_t1,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=get_start_info,  # 성공 시 시작시간 정보 저장
    params={'DATE': param_date},
    dag=dag)

t2 = SSHOperator(
    task_id='mariadb_to_json',
    command=command_t2,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)
    
t3 = SSHOperator(
    task_id='delete_datasource',
    command=command_t3,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)    

t4 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 10',
    on_failure_callback=result_msg, 
    dag=dag)

t5 = SSHOperator(
    task_id='config_change',
    command=command_t4,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag) 

t6 = SSHOperator(
    task_id='json_to_druid',
    command=command_t5,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag) 

#t8 = SSHOperator(
#    task_id='STUDY_config_change2',
#    command=command_t7,
#    ssh_hook=sshHook_imply_3113,
#    on_failure_callback=result_msg,  # 실패 시 정보전달
#    dag=dag) 

#t9 = SSHOperator(
#    task_id='STUDY_json_to_druid2',
#    command=command_t8,
#    ssh_hook=sshHook_imply_3113,
#    on_failure_callback=result_msg,  # 실패 시 정보전달
#    on_success_callback=result_msg,
#    dag=dag) 


t1.set_downstream(t2)
t2.set_downstream(t3)
t3.set_downstream(t4)
t4.set_downstream(t5)
t5.set_downstream(t6)
#t6.set_downstream(t7)
#t7.set_downstream(t8)
#t8.set_downstream(t9)
