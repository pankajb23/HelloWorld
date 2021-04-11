from airflow.models import Variable


class Config(object):
    def __init__(self,
                 team: str = None,
                 project: str = None,
                 schedule: str = None,
                 deployment: str = None,
                 dag_name: str = None,
                 email: str = None,
                 fabric: dict = None,
                 owner: str = None,
                 connId: str = None,
                 start_date: str = None,
                 timezone: str = None,
                 job_size: str = None,
                 custom_arg1: str = None,
                 start_date: str = None,
                 schedule_interval: str = None,
                 timezone: str = None):
        self.team = team
        self.project = project
        self.schedule = schedule
        self.deployment = deployment
        self.dag_name = dag_name
        self.owner = owner
        self.connId = connId
        self.fabric = fabric
        self.email = email
        self.job_size = job_size
        self.custom_arg1 = custom_arg1

        self.start_date = start_date or "2020-12-12T10:10:10"
        self.schedule_interval = schedule_interval or "hourly"
        self.timezone = timezone or "Europe/Warsaw"

    def dag_args(self):
        return {
            'dag_id': self.dag_name,
            'default_args': {
                "owner": self.owner,
                "email": [self.email],
                "depends_on_past": False,
                "start_date": self.start_date
            },
            "schedule_interval": self.schedule_interval,
            "timezone": self.timezone
        }
