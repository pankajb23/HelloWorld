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

@Visual(id = "PrepareComponent", label = "PrepareComponent", x = 588, y = 161, phase = 0, detailedStats = true)
object PrepareComponent {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      datediff(current_date(), col("account_open_date")).as("account_length_days"),
      col("order_id"),
      col("customer_id"),
      col("amount"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("email"),
      col("country_code"),
      col("account_flags")
    )

    out

  }

}
