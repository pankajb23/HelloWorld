
from airflow.models import BaseOperator
from airflow.operators.email_operator import EmailOperator

def Test_Email(config) -> BaseOperator:
    content = '''
    Test
    '''

    return EmailOperator(
        mime_charset = "utf-8",
        to = "Test",
        subject = "'Test'",
        html_content = content,
        task_id = "Test_Email",
        trigger_rule = "all_success"
    )
