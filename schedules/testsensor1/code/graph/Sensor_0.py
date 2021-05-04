from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.models import BaseOperator

def Sensor_0(config):
    sensor = S3KeySensor(
        bucket_key = "s3://abinitio-spark-redshift-testing/prophecy-libs-assembly-2.jar",
        wildcard_match = False,
        aws_conn_id = "aws_default_pankaj",
        verify = False,
        soft_fail = False,
        poke_interval = 60,
        timeout = 604800,
        mode = "poke",
        task_id = "Sensor_0",
        exponential_backoff = False
    )

    return sensor
