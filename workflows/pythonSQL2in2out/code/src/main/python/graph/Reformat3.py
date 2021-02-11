from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Reformat3", label = "Reformat3", x = 708, y = 191, phase = 0)
def Reformat3(spark: SparkSession, _in: DataFrame) -> Reformat:
    out = _in.select()

    return out
