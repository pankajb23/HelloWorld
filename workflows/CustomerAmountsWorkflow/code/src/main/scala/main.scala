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

import graph.SubGraph0._
import graph._

@Visual(mode = "batch", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_OrdersDatasetInput:    Source   = OrdersDatasetInput(spark)
    val df_CustomersDatasetInput: Source   = CustomersDatasetInput(spark)
    val df_My_Join_Component:     Join     = My_Join_Component(spark, df_OrdersDatasetInput, df_CustomersDatasetInput)
    val df_SubGraph0:             SubGraph = SubGraph0(spark,         df_My_Join_Component)
    CustomerOrdersDatasetOutput(spark, df_SubGraph0)

  }

  @Visual(id = "SubGraph0", label = "SubGraph0", x = 597, y = 106, phase = 0)
  def SubGraph0(spark: SparkSession, in: DataFrame): SubGraph = {

    val df_Reformat0: Reformat  = Reformat0(spark,  in)
    val out:          Aggregate = Aggregate0(spark, df_Reformat0)

    out

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark = SparkSession
      .builder()
      .appName("CustomerAmountsWorkflow")
      .config("spark.default.parallelism", "4")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
