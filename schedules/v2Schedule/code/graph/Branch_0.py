from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator


def Branch_0(config) -> BaseOperator:
    

    return BranchPythonOperator(
        task_id="Branch_0",
        op_kwargs={"config": config},
        python_callable=execute,
        trigger_rule="all_success"
    )


