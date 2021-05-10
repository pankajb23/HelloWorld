import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    op_wait_for_data = Wait_for_Data(config)
    op_decide = Decide(config)
    op_run_workflow = Run_Workflow(config)
    op_run_script = Run_Script(config)
    op_email_0 = Email_0(config)

    op_wait_for_data >> op_decide
    op_decide >> op_run_script
    op_decide >> op_run_workflow[0]
    op_run_script >> op_email_0
    op_run_workflow[1] >> op_email_0


