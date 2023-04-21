import json
import requests

# 메시지 전송
def send_message(name, message):
    url = "https://h-support.hunet.name/api/webhook"
    payload = {"info": name, "message" : message, "targets" : ["2021028"] }
    
    requests.post(url, json=payload)


def failed_callback(context):
    # message 작성
    message = """
            :red_circle: Task Failed.
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {exec_date}
            *Exception*: {exception}
            *Log Url*: {log_url}
            """.format(
        dag=context.get('task_instance').dag_id,
        task=context.get('task_instance').task_id,
        exec_date=context.get('execution_time'),
        exception=context.get('exception'),
        log_url=context.get('task_instance').log_url
    )
    send_message('elasticsearch', message)
