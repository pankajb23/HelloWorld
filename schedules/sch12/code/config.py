


class Config(object):

    def __init__(self, fabric: str="common"):
        self.fabric = fabric
        self.dag_id = "sch12"
        self.timezone = ""
        self.start_date = ""
        self.schedule_interval = ""
        self.poke_interval = "55"
        self.cluster_id = "eng.pankaj.002@prophecy.io_49_connId"

    def dag_args(self):
        return {
            'dag_id': self.dag_id,
            'default_args': {
"depends_on_past" : False, "start_date" : self.start_date},
            "schedule_interval": self.schedule_interval
        }
