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

@Visual(id = "Target0", label = "Target0", x = 1443, y = 167, phase = 0)
object Target0 {

  @UsesDataset(id = "137", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "azdb" =>
        val schemaArg = StructType(
          Array(StructField("country_code", StringType, false), StructField("amount", DoubleType, false))
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
