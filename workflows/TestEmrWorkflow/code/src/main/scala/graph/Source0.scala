package graph

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
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import graph._

@Visual(id = "Source0", label = "Source0", x = 170, y = 50, phase = 0)
object Source0 {

  @UsesDataset(id = "199", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "emr" =>
        spark.read
          .format("csv")
          .option("header",      true)
          .option("sep",         ",")
          .option("inferSchema", true)
          .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/OrdersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
