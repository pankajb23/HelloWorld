package graph

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

@Visual(id = "OrdersDatasetInput", label = "OrdersDatasetInput", x = 6, y = 42, phase = 0)
object OrdersDatasetInput {

  @UsesDataset(id = "16", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "emr2" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",       IntegerType,   true),
            StructField("customer_id",    IntegerType,   true),
            StructField("order_status",   StringType,    true),
            StructField("order_category", StringType,    true),
            StructField("order_date",     TimestampType, true),
            StructField("amount",         DoubleType,    true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/OrdersDatasetInput.csv")
          .cache()
      case "narayan" =>
        spark.read
          .format("parquet")
          .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomerOrdersDatasetOutput.csv/")
          .cache()
      case "azdbdp1" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",       IntegerType, true),
            StructField("customer_id",    IntegerType, true),
            StructField("order_status",   StringType,  true),
            StructField("order_category", StringType,  true),
            StructField("order_date",     StringType,  true),
            StructField("amount",         DoubleType,  true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("dbfs:/Users/prophecy/abhishek/OrdersDatasetInput.csv")
          .cache()
      case "livy" =>
        spark.read
          .format("csv")
          .option("header",      true)
          .option("sep",         ",")
          .option("inferSchema", true)
          .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/OrdersDatasetInput.csv")
          .cache()
      case "azdb" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",       IntegerType, false),
            StructField("customer_id",    IntegerType, false),
            StructField("order_status",   StringType,  false),
            StructField("order_category", StringType,  false),
            StructField("order_date",     StringType,  false),
            StructField("amount",         DoubleType,  false)
          )
        )
        spark.read
          .format("csv")
          .option("sep", ",")
          .schema(schemaArg)
          .load("dbfs:/Users/prophecy/eng/OrdersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
