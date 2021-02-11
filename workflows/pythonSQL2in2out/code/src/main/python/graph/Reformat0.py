from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Reformat0", label = "Reformat0", x = 515, y = 82, phase = 0)
def Reformat0(spark: SparkSession, _in: DataFrame) -> Reformat:
    out = _in.select()

    return out
