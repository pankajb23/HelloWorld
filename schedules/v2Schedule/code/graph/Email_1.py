
from airflow.models import BaseOperator
from airflow.operators.email_operator import EmailOperator


def Email_1(config) -> BaseOperator:
    content = '''
    Test email
    '''

    return EmailOperator(
        mime_charset="utf-8",
        to="rajat@prophecy.io",
        subject="'Delta'",
        html_content=content,
        task_id="Email_1",
        trigger_rule="all_success"
    )
