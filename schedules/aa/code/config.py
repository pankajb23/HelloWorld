


class Config(object):

    def __init__(self, fabric: str="common"):
        self.fabric = fabric
        self.dag_id = "aa"
        self.timezone = "US/Pacific"
        self.start_date = "2021-05-20T07:03:49"
        self.schedule_interval = "@daily"

    def dag_args(self):
        return {
            'dag_id': self.dag_id,
            'default_args': {
"depends_on_past" : False, "start_date" : self.start_date},
            "schedule_interval": self.schedule_interval
        }
