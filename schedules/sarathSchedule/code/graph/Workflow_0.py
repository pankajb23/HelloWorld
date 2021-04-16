from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
def Workflow_0(config):
    if config.fabric == "azdb":
        workflow_id = ""
        workflow_version = "latest"
        workflow_jar = "s3://abinitio-spark-redshift-testing/prophecy/jars//latest/workflow.jar"
        prophecy_libs_jar = "s3://abinitio-spark-redshift-testing/prophecy/jars/libs/version/prophecy-libs-assembly-1.0.jar"
        workflow = DatabricksSubmitRunOperator(task_id = "Workflow_0", new_cluster = "Small", spark_jar_task = {"main_class_name": "main", "parameters": ["-C", "fabricName=" + config.fabric]}, databricks_conn_id = config.connection_id, libraries = {"jar": workflow_jar, "jar": prophecy_libs_jar})
        return workflow, workflow
