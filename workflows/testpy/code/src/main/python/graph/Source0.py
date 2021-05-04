from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Source0", label = "Source0", x = 170, y = 50, phase = 0)
@UsesDataset(id = "17", version = 0)
def Source0(spark: SparkSession) -> Source:
    fabric = Config.fabricName
    out = None

    if fabric == "Delta":
        out = spark.read.format("parquet").option("location", "heds").load("heds")
    elif fabric == "emr2":
        schemaArg = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("country_code", StringType(), False),
            StructField("order_id", LongType(), False),
            StructField("amount", DoubleType(), False)
        ])
        out = spark.read\
                  .format("parquet")\
                  .schema(schemaArg)\
                  .option("location", "s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomerDatasetOutput2.csv/")\
                  .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomerDatasetOutput2.csv/")
    elif fabric == "narayan":
        out = spark.read.format("parquet").option("location", "mmm").load("mmm")
    elif fabric == "awsdbold":
        out = spark.read.format("parquet").option("location", "testing12.3.com").load("testing12.3.com")
    elif fabric == "emr":
        out = spark.read.format("parquet").option("location", "qwq").load("qwq")
    elif fabric == "azdbdp1":
        schemaArg = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("country_code", StringType(), False),
            StructField("order_id", LongType(), False),
            StructField("amount", DoubleType(), False)
        ])
        out = spark.read\
                  .format("csv")\
                  .schema(schemaArg)\
                  .load("dbfs:/Users/prophecy/eng/CustomerOrdersDatasetOutput7.csv/")
    elif fabric == "awsnewdp1":
        schemaArg = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("country_code", StringType(), False),
            StructField("order_id", LongType(), False),
            StructField("amount", DoubleType(), False)
        ])
        out = spark.read\
                  .format("csv")\
                  .schema(schemaArg)\
                  .load("dbfs:/Prophecy/rajat@prophecy.io/CustomerOrdersDatasetOutput1.csv/")
    elif fabric == "livy":
        schemaArg = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("country_code", StringType(), False),
            StructField("order_id", LongType(), False),
            StructField("amount", DoubleType(), False)
        ])
        out = spark.read\
                  .format("csv")\
                  .schema(schemaArg)\
                  .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomerOrdersDatasetOutput.csv/")
    elif fabric == "azdb":
        schemaArg = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("country_code", StringType(), False),
            StructField("orders", LongType(), False),
            StructField("amount", DoubleType(), False)
        ])
        out = spark.read\
                  .format("parquet")\
                  .schema(schemaArg)\
                  .option("location", "dbfs:/Users/prophecy/eng/CustomerOrdersDatasetOutput6.csv")\
                  .load("dbfs:/Users/prophecy/eng/CustomerOrdersDatasetOutput6.csv")
    else:
        raise ValueError("The fabric %s is not supported" % fabric)

    return out
