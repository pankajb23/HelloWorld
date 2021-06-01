import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

import graph._

@Visual(mode = "batch", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_OrdersDatasetInput: Source = OrdersDatasetInput(spark)

    val df_CustomersDatasetInput: Source = CustomersDatasetInput(spark)
    val df_My_Join_Component:     Join   = My_Join_Component(spark, df_OrdersDatasetInput, df_CustomersDatasetInput)
    CustomerOrdersDatasetOutput(spark, df_My_Join_Component)

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

    UDFs.registerUDFs(spark)
    UDAFs.registerUDAFs(spark)

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
