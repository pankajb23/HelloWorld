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

@Visual(id = "SQLStatement0", label = "SQLStatement0", x = 366, y = 155, phase = 0)
object SQLStatement0 {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame): (SQLStatement, SQLStatement) = {
    import spark.implicits._

    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    val out0 = spark.sql("select * from in0")
    val out1 = spark.sql("select * from in1")

    (out0, out1)

  }

}
