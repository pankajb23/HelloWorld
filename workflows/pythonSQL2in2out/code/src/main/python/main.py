from pyspark.sql import SparkSession


class Main:

    def graph(self, spark: SparkSession):
        df_Source0: Source = Source0(spark)
        df_Source1: Source = Source1(spark)
        df_SQLStatement0_0, df_SQLStatement0_1 = SQLStatement0(spark, df_Source0, df_Source1)
        df_Reformat1: Reformat = Reformat1(spark, df_SQLStatement0_1)
        df_Reformat3: Reformat = Reformat3(spark, df_Reformat1)
        df_Reformat0: Reformat = Reformat0(spark, df_SQLStatement0_0)
        df_Reformat2: Reformat = Reformat2(spark, df_Reformat0)

    def main(self):
        spark = SparkSession.builder.appName("pythonSQL2in2out").getOrCreate()
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        self.graph(spark)

if __name__ == __main__:
    Main().main()
