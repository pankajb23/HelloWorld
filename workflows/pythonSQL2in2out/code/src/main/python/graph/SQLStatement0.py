from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "SQLStatement0", label = "SQLStatement0", x = 372, y = 134, phase = 0)
def SQLStatement0(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (SQLStatement, SQLStatement):
    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    out0 = spark.sql("select * from in0")
    out1 = spark.sql("select * from in1")

    return out0, out1
