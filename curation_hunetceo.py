# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta
import airflow
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.trigger_rule import TriggerRule
# DAGs 의 성공/실패 정보를 Kafka API 서버에 전달하는 Custom Operator import
try:
    from KafkaRestApiCallOperator import KafkaRestApiCallOperator
except ImportError:
    from airflow.operators import KafkaRestApiCallOperator


sshHook_m2d2_5135 = SSHHook(
    ssh_conn_id='m2d2_5135'
)

sshHook_curation_2321 = SSHHook(
    ssh_conn_id='curation_2321'
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['sy.lee@hunet.co.kr'],
    'email_on_failure': False,
    'email_on_retry': False
}

# schedule_interval=timedelta(days=1)
# * * * * *
# 분 시 일 월 요일
dag = DAG(
    'curation_hunetceo',
    default_args=default_args,
    description='휴넷CEO 과정 대상 추천시스템',
    schedule_interval='00 18 * * *'  # 오전 3시 30분
)


# 휴넷 CEO 기업별 수강생 학습 정보
command_t1 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_curation_hunetceo_study_user_mssql_to_s3.py"

# 휴넷 CEO 과정정보
command_t2 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_curation_hunetceo_info_mssql_to_s3.py"

# 기업별 학습자 정보
command_t3 = "/home/hunetdb/anaconda3/envs/dbadmin/bin/python " \
             "/home/hunetdb/anaconda3/envs/dbadmin/script/get_curation_company_user_mssql_to_s3.py"


command_t4 = "/home/hunetdb/anaconda3/envs/curation/bin/python " \
             "/home/hunetdb/src/script/hunetceo/IBRS/process_match_email.py "

def get_start_info(context):
    """
    처음 시작되는 task의 시작 시간을 조회하여, xcom 영역에 저장한다.
    :param context: Airflow context 객체
    :return:
    """
    context['ti'].xcom_push(key="start_info", value={
        "dag_id": context['ti'].dag_id,
        "hostname": str(context['ti'].hostname),
        "start_time": context['ti'].start_date.strftime("%Y-%m-%dT%H:%M:%S")
    })


def result_msg(context):
    """
    성공/실패 시 kafka.hunet.co.kr(10.140.90.51) Kafka Api 서버에 결과 값을 전달 한다.
    :param context: Airflow context 객체
    :return:
    """
    kafka_msg_send = KafkaRestApiCallOperator(task_id="kafka_restapi_msg_send", endpoint="/kafka_api")
    kafka_msg_send.execute(context)


# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = SSHOperator(
    task_id='hunetceo_study_user',
    command=command_t1,
    ssh_hook=sshHook_m2d2_5135,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=get_start_info,  # 성공 시 시작시간 정보 저장
    dag=dag)
    
t2 = SSHOperator(
    task_id='hunetceo_info',
    command=command_t2,
    ssh_hook=sshHook_m2d2_5135,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)

t3 = SSHOperator(
    task_id='company_user',
    command=command_t3,
    ssh_hook=sshHook_m2d2_5135,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    dag=dag)    

t4 = SSHOperator(
    task_id='send_recommendation_email',
    command=command_t4,
    ssh_hook=sshHook_curation_2321,
    on_failure_callback=result_msg,  # 실패 시 정보전달
    on_success_callback=result_msg,
    dag=dag)    


t1.set_downstream(t2)
t2.set_downstream(t3)
t3.set_downstream(t4)