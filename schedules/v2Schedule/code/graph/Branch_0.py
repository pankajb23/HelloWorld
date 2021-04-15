from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator
from airflow.operators.python_operator import BranchPythonOperator

def execute1(config, **kwargs):
        return 'Some script component'


def Branch_0(config) -> BaseOperator:
    

    return BranchPythonOperator(
        task_id="Branch_0",
        op_kwargs={"config": config},
        python_callable=execute1,
        trigger_rule="all_success"
    )


