from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator

def Script_1(config) -> BaseOperator:

    def execute(config, **kwargs):
        # ===================================
        # Place any Python code here
        # ===================================
        return "Finished"

    return PythonOperator(
        task_id = "Script_1",
        op_kwargs = {"config" : config},
        python_callable = execute,
        trigger_rule = "all_success"
    )
