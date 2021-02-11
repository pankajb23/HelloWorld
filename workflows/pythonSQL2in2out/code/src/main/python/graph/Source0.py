from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Source0", label = "Source0", x = 149, y = 50, phase = 0)
@UsesDataset(id = "15", version = 0)
def Source0(spark: SparkSession) -> Source:
    fabric = Config.fabricName
    out = None

    if fabric == "azdb":
        schemaArg = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("email", StringType(), False),
            StructField("country_code", StringType(), False),
            StructField("account_open_date", StringType(), False),
            StructField("account_flags", StringType(), False)
        ])
        out = spark.read\
                  .format("csv")\
                  .schema(schemaArg)\
                  .option("sep", ",")\
                  .option("inferSchema", True)\
                  .load("dbfs:/Users/prophecy/eng/CustomersDatasetInput.csv")
    else:
        raise ValueError("The fabric %s is not supported" % fabric)

    return out
