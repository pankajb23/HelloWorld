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

@Visual(id = "CustomerOrdersDatasetOutput", label = "CustomerOrdersDatasetOutput", x = 1486, y = 100, phase = 0)
object CustomerOrdersDatasetOutput {

  @UsesDataset(id = "17", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "azdb" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",  IntegerType, false),
            StructField("country_code", StringType,  false),
            StructField("orders",       LongType,    false),
            StructField("amount",       DoubleType,  false)
          )
        )
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("overwrite")
          .save("dbfs:/Users/prophecy/eng/CustomerOrdersDatasetOutput5.csv")
      case "azdbdp1" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",  IntegerType, false),
            StructField("country_code", StringType,  false),
            StructField("orders",       LongType,    false),
            StructField("amount",       DoubleType,  false)
          )
        )
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("overwrite")
          .save("dbfs:/Users/prophecy/eng/CustomerOrdersDatasetOutput5.csv")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
