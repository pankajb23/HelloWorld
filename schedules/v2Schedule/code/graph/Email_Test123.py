
from airflow.models import BaseOperator
from airflow.operators.email_operator import EmailOperator


def Email_Test123(config) -> BaseOperator:
    content = '''
    Delta
    '''

    return EmailOperator(
        mime_charset="utf-8",
        to="Delta@prophecy.io",
        subject="'Delta'",
        html_content=content,
        task_id="Email_Test123",
        trigger_rule="all_success"
    )
