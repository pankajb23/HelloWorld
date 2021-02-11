from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Reformat1", label = "Reformat1", x = 530, y = 229, phase = 0)
def Reformat1(spark: SparkSession, _in: DataFrame) -> Reformat:
    out = _in.select()

    return out
