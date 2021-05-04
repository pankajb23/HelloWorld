from pyspark.sql import SparkSession


class Main:

    def graph(self, spark: SparkSession):
        df_Source0: Source = Source0(spark)

    def main(self):
        spark = SparkSession.builder().appName("testpy").enableHiveSupport().getOrCreate()
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        self.graph(spark)

if __name__ == __main__:
    Main().main()
