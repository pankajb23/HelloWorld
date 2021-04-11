from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Reformat2", label = "Reformat2", x = 669, y = 42, phase = 0)
def Reformat2(spark: SparkSession, _in: DataFrame) -> Reformat:
    out = _in.select()

    return out
