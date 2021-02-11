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

@Visual(id = "Reformat1", label = "Reformat1", x = 792, y = 258, phase = 0, detailedStats = true)
object Reformat1 {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      col("customer_id"),
      col("last_name"),
      col("country_code")
    )

    out

  }

}
