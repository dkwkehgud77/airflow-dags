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
    'dw',
    default_args=default_args,
    description='회원, 마이크로러닝, 매출, 진도 데이터 -> MariaDB, Druid 적재',
    schedule_interval='20 17 * * *' # 오전 2:20
    # schedule_interval='52 07 * * *'
)

# [DW] 회원
command_t1 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_user_info_mssql_to_mariadb.py {{params.DATE}}"

command_t2 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_user_info_mariadb_to_json.py {{params.DATE}}"

command_t3 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-user-info.py "

command_t4 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-user-info/dw-user-info.json "

# [DW] 회원 통계
command_t5 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_user_stats_info_mssql_to_mariadb.py {{params.DATE}}"

command_t6 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_user_stats_info_mariadb_to_json.py {{params.DATE}}"

command_t7 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-user-stats-info.py "

command_t8 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-user-stats-info/dw-user-stats-info.json "


# [DW] 학습_마이크로러닝
command_t9 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_study_micro_info_mssql_to_mariadb.py {{params.DATE}}"

command_t10 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_study_micro_info_mariadb_to_json.py {{params.DATE}}"

command_t11 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-study-micro-info.py "

command_t12 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-study-micro-info/dw-study-micro-info.json "



# [DW] 매출
command_t13 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_sales_info_mssql_to_mariadb.py {{params.DATE}}"

command_t14 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_sales_info_mariadb_to_json.py {{params.DATE}}"

command_t15 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-sales-info.py {{params.DATE}}"

command_t16 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-sales-info/dw-sales-info.json "

# [DW] 차시
command_t17 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_study_progress_info_mssql_to_mariadb.py {{params.DATE}}"

command_t18 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_hunet_dw_study_progress_info_mariadb_to_json.py {{params.DATE}}"

command_t19 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python /home/hunetdb/imply/conf-hunet/druid-batch-config-dw-study-progress-info.py {{params.DATE}}"

command_t20 = "/home/hunetdb/imply/bin/post-index-task --file /home/hunetdb/imply/conf-hunet/druid-batch-config/dw-study-progress-info/dw-study-progress-info.json "


# # [DW] Old file delete
# command_t14 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
#               "/home/hunetdb/anaconda3/envs/dbadmin/script/old_file_delete.py"


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
    task_id='USER_mssql_to_mariadb',
    command=command_t1,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=get_start_info,  # 성공 시 시작시간 정보 저장
    params={'DATE': param_date},
    dag=dag)

t2 = SSHOperator(
    task_id='USER_mariadb_to_json',
    command=command_t2,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)
    
t3 = SSHOperator(
    task_id='USER_change_config',
    command=command_t3,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t4 = SSHOperator(
    task_id='USER_json_to_druid',
    command=command_t4,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)    

t5 = SSHOperator(
    task_id='USERSTATS_mssql_to_mariadb',
    command=command_t5,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t6 = SSHOperator(
    task_id='USERSTATS_mariadb_to_json',
    command=command_t6,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)
    
t7 = SSHOperator(
    task_id='USERSTATS_change_config',
    command=command_t7,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t8 = SSHOperator(
    task_id='USERSTATS_json_to_druid',
    command=command_t8,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)   

t9 = SSHOperator(
    task_id='MICRO_mssql_to_mariadb',
    command=command_t9,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t10 = SSHOperator(
    task_id='MICRO_mariadb_to_json',
    command=command_t10,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)
    
t11 = SSHOperator(
    task_id='MICRO_change_config',
    command=command_t11,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t12 = SSHOperator(
    task_id='MICRO_json_to_druid',
    command=command_t12,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)   

t13 = SSHOperator(
    task_id='SALES_mssql_to_mariadb',
    command=command_t13,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t14 = SSHOperator(
    task_id='SALES_mariadb_to_json',
    command=command_t14,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)
    
t15 = SSHOperator(
    task_id='SALES_change_config',
    command=command_t15,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t16 = SSHOperator(
    task_id='SALES_json_to_druid',
    command=command_t16,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)

t17 = SSHOperator(
    task_id='STUDY_PROGRESS_mssql_to_mariadb',
    command=command_t17,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t18 = SSHOperator(
    task_id='STUDY_PROGRESS_mariadb_to_json',
    command=command_t18,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)
    
t19 = SSHOperator(
    task_id='STUDY_PROGRESS_change_config',
    command=command_t19,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    params={'DATE': param_date},
    dag=dag)

t20 = SSHOperator(
    task_id='STUDY_PROGRESS_json_to_druid',
    command=command_t20,
    ssh_hook=sshHook_imply_3113,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=result_msg,
    dag=dag)
    
t1.set_downstream(t2)
t2.set_downstream(t3)
t3.set_downstream(t4)
t4.set_downstream(t5)
t5.set_downstream(t6)
t6.set_downstream(t7)
t7.set_downstream(t8)
t8.set_downstream(t9)
t9.set_downstream(t10)
t10.set_downstream(t11)
t11.set_downstream(t12)
t12.set_downstream(t13)
t13.set_downstream(t14)
t14.set_downstream(t15)
t15.set_downstream(t16)
t16.set_downstream(t17)
t17.set_downstream(t18)
t18.set_downstream(t19)
t19.set_downstream(t20)

#t4.set_downstream(t14)
# t5.set_downstream(t6)
# t6.set_downstream(t7)
# t7.set_downstream(t8)
# t8.set_downstream(t14)