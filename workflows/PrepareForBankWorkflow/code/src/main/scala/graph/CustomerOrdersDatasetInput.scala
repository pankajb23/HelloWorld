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

@Visual(id = "CustomerOrdersDatasetInput", label = "CustomerOrdersDatasetInput", x = 6, y = 97, phase = 0)
object CustomerOrdersDatasetInput {

  @UsesDataset(id = "137", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "azdb" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",            IntegerType, true),
            StructField("orders",              IntegerType, true),
            StructField("amount",              DoubleType,  true),
            StructField("customer_id",         IntegerType, true),
            StructField("first_name",          StringType,  true),
            StructField("last_name",           StringType,  true),
            StructField("phone",               StringType,  true),
            StructField("email",               StringType,  true),
            StructField("country_code",        StringType,  true),
            StructField("account_length_days", IntegerType, true),
            StructField("account_flags",       StringType,  true)
          )
        )
        spark.read
          .format("csv")
          .option("sep", ",")
          .schema(schemaArg)
          .load("dbfs:/Users/prophecy/eng/CustomerOrdersDatasetOutput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
