from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator
from airflow.operators.python_operator import BranchPythonOperator

def Branch_0(config) -> BaseOperator:

    def execute(config, **kwargs):
        # ===================================
        # Place your branching strategy here. Only the task ids that this function returns
        # will be executed.
        # It can return a single task_id as a string (e.g. `return "next_task_id"`) or
        # a list with many task ids (e.g. `return ["task_id_1", "task_id_2"]`).
        # ===================================
        return "next_task_id"

    return BranchPythonOperator(
        task_id = "Branch_0",
        op_kwargs = {"config" : config},
        python_callable = execute,
        trigger_rule = "all_success"
    )
