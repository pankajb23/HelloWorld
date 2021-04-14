package graph.SubGraph0

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

@Visual(id = "Aggregate0", label = "Aggregate0", x = 698, y = 344, phase = 0)
object Aggregate0 {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {
    import spark.implicits._

    val dfGroupBy = in.groupBy(col("customer_id").as("customer_id"),     col("country_code").as("country_code"))
    val out       = dfGroupBy.agg(count(col("order_id")).as("order_id"), sum(col("amount")).as("amount"))

    out

  }

}
