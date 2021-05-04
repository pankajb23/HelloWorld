from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

def Workflow_0(config):
    if config.fabric == "emr":
        workflow_id = "98"
        workflow_version = "latest"
        workflow_jar = f"s3://{config.s3Bucket}/prophecy/jars/98/latest/workflow.jar"
        prophecy_libs_jar = f"{config.prophecyLibsJar}"
        executor_memory = "1g"
        executor_cores = "4"
        num_executors = "6"
        driver_memory = "1g"
        driver_cores = "2"
        job_flow_id = config.cluster_id
        spark_steps = [{
                         "Name": "Compute_Step",
                         "ActionOnFailure": "CONTINUE",
                         "HadoopJarStep": {
                           "Jar": "command-runner.jar",
                           "Args": ["spark-submit", "--executor-memory", executor_memory, "--executor-cores",
                                     executor_cores, "--num-executors",
                                     num_executors, "--driver-memory",
                                     driver_memory, "--driver-cores", driver_cores,
                                     "--conf",
                                     "spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4",
                                     "--conf",
                                     "spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4",
                                     "--deploy-mode", "cluster", "--class", "Main",
                                     "--jars", workflow_jar, prophecy_libs_jar,
                                     "-C", "runDate={{ds}}", "-C",
                                     "fabricName=" + config.fabric]
                         }
                       }]
        step_adder = EmrAddStepsOperator(
            task_id = "Workflow_0",
            job_flow_id = job_flow_id,
            aws_conn_id = "aws_default_pankaj",
            steps = spark_steps,
            trigger_rule = "all_success"
        )
        step_checker = EmrStepSensor(
            task_id = "Workflow_0WatchSteps",
            job_flow_id = job_flow_id,
            step_id = "{{ task_instance.xcom_pull(task_ids='Workflow_0', key='return_value')[0] }}",
            aws_conn_id = "aws_default_pankaj"
        )
        step_adder >> step_checker

        return step_adder, step_checker
