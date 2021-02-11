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

@Visual(id = "Join0", label = "Join0", x = 336, y = 118, phase = 0, detailedStats = true)
object Join0 {

  def apply(spark: SparkSession, left: DataFrame, right: DataFrame): Join = {
    import spark.implicits._

    val leftAlias  = left.as("left")
    val rightAlias = right.as("right")
    val dfJoin     = leftAlias.join(rightAlias, col("left.customer_id") === col("right.customer_id"), "inner")

    val out = dfJoin.select(
      col("right.order_id").as("order_id"),
      col("right.customer_id").as("customer_id"),
      col("right.amount").as("amount"),
      col("left.first_name").as("first_name"),
      col("left.last_name").as("last_name"),
      col("left.email").as("email"),
      col("left.phone").as("phone"),
      col("left.country_code").as("country_code"),
      col("left.account_open_date").as("account_open_date"),
      col("left.account_flags").as("account_flags")
    )

    out

  }

}
