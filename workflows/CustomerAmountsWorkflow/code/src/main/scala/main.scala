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

    val df_CustomersDatasetInput: Source      = CustomersDatasetInput(spark)
    val df_OrdersDatasetInput:    Source      = OrdersDatasetInput(spark)
    val df_JoinComponent:         Join        = JoinComponent(spark,      df_OrdersDatasetInput, df_CustomersDatasetInput)
    val df_PrepareComponent:      Reformat    = PrepareComponent(spark,   df_JoinComponent)
    val df_AggregateComponent:    Aggregate   = AggregateComponent(spark, df_PrepareComponent)
    val df_Repartition0:          Repartition = Repartition0(spark,       df_AggregateComponent)
    val df_Filter0:               Filter      = Filter0(spark,            df_Repartition0)
    CustomerOrdersDatasetOutput(spark, df_Filter0)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("CustomerAmountsWorkflow")
      .config("spark.default.parallelism", 4)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
