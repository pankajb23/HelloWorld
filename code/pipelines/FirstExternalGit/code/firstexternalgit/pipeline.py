from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from firstexternalgit.config.ConfigStore import *
from firstexternalgit.udfs.UDFs import *
from prophecy.utils import *
from firstexternalgit.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Reformat_1 = Reformat_1(spark)
    df_Reformat_2 = Reformat_2(spark, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/FirstExternalGit")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/FirstExternalGit", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/FirstExternalGit")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
