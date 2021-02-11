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
import graph._

@Visual(id = "RowDistributor0", label = "RowDistributor0", x = 814, y = 104, phase = 0)
object RowDistributor0 {

  def apply(spark: SparkSession, in: DataFrame): (RowDistributor, RowDistributor) = {
    import spark.implicits._

    val out0 = in.filter(col("country_code") === lit("CN"))
    val out1 = in.filter(col("country_code") === lit("ID"))

    (out0, out1)

  }

}
