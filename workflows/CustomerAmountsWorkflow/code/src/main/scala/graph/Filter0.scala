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

@Visual(id = "Filter0", label = "Filter0", x = 811, y = 28, phase = 0)
object Filter0 {

  def apply(spark: SparkSession, in: DataFrame): Filter = {
    import spark.implicits._

    val out = in.filter(col("country_code") === lit("CN"))

    out

  }

}
