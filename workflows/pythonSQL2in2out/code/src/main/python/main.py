sdl.core.codeparse.CodeParseException: Code Parsing error at token {pass} with probable reason "indent expected" 
from pyspark.sql import SparkSession
class Main:
    
    def graph(self, spark: SparkSession):
    pass
    def main(self):
        spark = SparkSession.builder.appName("pythonSQL2in2out").getOrCreate()
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        self.graph(spark)
if __name__ == __main__:
    Main().main()
