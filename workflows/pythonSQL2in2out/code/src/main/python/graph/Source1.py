from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Source1", label = "Source1", x = 156, y = 198, phase = 0)
@UsesDataset(id = "16", version = 0)
def Source1(spark: SparkSession) -> Source:
    fabric = Config.fabricName
    out = None

    if fabric == "azdb":
        schemaArg = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("order_status", StringType(), False),
            StructField("order_category", StringType(), False),
            StructField("order_date", StringType(), False),
            StructField("amount", DoubleType(), False)
        ])
        out = spark.read\
                  .format("csv")\
                  .schema(schemaArg)\
                  .option("sep", ",")\
                  .option("inferSchema", True)\
                  .load("dbfs:/Users/prophecy/eng/OrdersDatasetInput.csv")
    else:
        raise ValueError("The fabric %s is not supported" % fabric)

    return out
