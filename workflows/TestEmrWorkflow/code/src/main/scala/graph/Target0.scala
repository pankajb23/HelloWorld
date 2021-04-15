package graph

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
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import graph._

@Visual(id = "Target0", label = "Target0", x = 682, y = 48, phase = 0)
object Target0 {

  @UsesDataset(id = "200", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "emr" =>
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("overwrite")
          .save("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/TestPankajOuput.csv/")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
