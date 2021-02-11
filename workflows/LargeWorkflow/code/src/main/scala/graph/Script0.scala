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

@Visual(id = "Script0", label = "Script0", x = 361, y = 56, phase = 0)
object Script0 {

  def apply(spark: SparkSession, in0: DataFrame): Script = {
    import spark.implicits._

    val out0 = in0.select((0 until 900).map(idx => col("first_name").as(s"col${idx}")).toSeq: _*)

    out0

  }

}
