package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "My_Join_Component", label = "My Join Component", x = 260, y = 210, phase = 0)
object My_Join_Component {

  def apply(spark: SparkSession, left: DataFrame, right: DataFrame): Join = {
    import spark.implicits._

    val leftAlias  = left.as("left")
    val rightAlias = right.as("right")
    val dfJoin     = leftAlias.join(rightAlias, col("left.customer_id") === col("right.customer_id"), "inner")

    val out = dfJoin.select(
      col("right.account_open_date").as("account_open_date"),
      col("left.order_id").as("order_id"),
      col("left.customer_id").as("customer_id"),
      col("left.amount").as("amount"),
      col("left.order_status").as("order_status"),
      col("right.first_name").as("first_name"),
      col("right.last_name").as("last_name"),
      col("right.phone").as("phone"),
      col("right.email").as("email"),
      col("right.country_code").as("country_code"),
      col("right.account_flags").as("account_flags")
    )

    out

  }

}
