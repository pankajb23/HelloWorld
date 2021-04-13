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
import udfs.UDFs._

import graph._

@Visual(mode = "stream", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_Source0:   Source   = Source0(spark)
    val df_Reformat0: Reformat = Reformat0(spark, df_Source0)
    Target0(spark, df_Reformat0)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark = SparkSession.builder().appName("teststreaming").enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
