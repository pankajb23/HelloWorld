
from airflow.models import BaseOperator
from airflow.operators.email_operator import EmailOperator

def Email_0(config) -> BaseOperator:
    content = '''
    Yay
    '''

    return EmailOperator(
        mime_charset = "utf-8",
        to = "rajat@prophecy.io",
        subject = "'Success'",
        html_content = content,
        task_id = "Email_0",
        trigger_rule = "one_success"
    )
