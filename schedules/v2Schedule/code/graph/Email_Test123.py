
from airflow.models import BaseOperator
from airflow.operators.email_operator import EmailOperator


def Email_Test123(config) -> BaseOperator:
    content = '''
    
    '''

    return EmailOperator(
        mime_charset="utf-8",
        to="",
        subject="''",
        html_content=content,
        task_id="email_notification",
        trigger_rule="all_success"
    )
