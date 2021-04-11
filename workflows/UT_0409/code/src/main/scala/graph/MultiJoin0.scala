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

@Visual(id = "MultiJoin0", label = "MultiJoin0", x = 451, y = 521, phase = 0)
object MultiJoin0 {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame): MultiJoin = {
    import spark.implicits._

    val df_in0 = in0.as("in0")
    val df_in1 = in1.as("in1")
    val df_in2 = in2.as("in2")

    val df_join0 = df_in0.join(df_in1, col("in1.customer_id") === col("in0.customer_id"), "inner")

    val df_join1 = df_join0.join(df_in2, col("in2.customer_id") === col("in0.customer_id"), "inner")

    val out = df_join1.select(
      col("in0.customer_id").as("customer_id"),
      col("in0.email").as("email"),
      col("in0.country_code").as("country_code"),
      col("in0.account_open_date").as("account_open_date"),
      col("in0.account_flags").as("account_flags"),
      col("in1.customer_id").as("customer_id"),
      col("in1.first_name").as("first_name"),
      col("in1.last_name").as("last_name"),
      col("in1.phone").as("phone"),
      col("in1.email").as("email"),
      col("in1.country_code").as("country_code"),
      col("in1.account_open_date").as("account_open_date"),
      col("in1.account_flags").as("account_flags")
    )

    out

  }

}
