import org.apache.spark.sql.types._
import io.prophecy.libs._
import io.prophecy.libs.UDFUtils._
import io.prophecy.libs.Component._
import io.prophecy.libs.DataHelpers._
import io.prophecy.libs.SparkFunctions._
import io.prophecy.libs.FixedFileFormatImplicits._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._

import graph._

@Visual(mode = "batch", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_Customers:  Source    = Customers(spark)
    val df_Orders:     Source    = Orders(spark)
    val df_Join0:      Join      = Join0(spark,      df_Customers, df_Orders)
    val df_Reformat0:  Reformat  = Reformat0(spark,  df_Join0)
    val df_Reformat1:  Reformat  = Reformat1(spark,  df_Reformat0)
    val df_Reformat2:  Reformat  = Reformat2(spark,  df_Reformat1)
    val df_Aggregate0: Aggregate = Aggregate0(spark, df_Reformat0)
    Target0(spark, df_Aggregate0)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("detailedstatsexperiments")
      .config("spark.default.parallelism", 4)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
