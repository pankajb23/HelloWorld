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

@Visual(id = "CustomerOrdersDatasetOutput", label = "CustomerOrdersDatasetOutput", x = 449, y = 210, phase = 0)
object CustomerOrdersDatasetOutput {

  @UsesDataset(id = "17", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    Config.fabricName match {
      case "Delta" =>
        in.write
          .format("parquet")
          .mode("overwrite")
          .save("heds")
      case "emr2" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",  IntegerType, false),
            StructField("country_code", StringType,  false),
            StructField("order_id",     LongType,    false),
            StructField("amount",       DoubleType,  false)
          )
        )
        in.write
          .format("parquet")
          .mode("overwrite")
          .save("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomerDatasetOutput2.csv/")
      case "narayan" =>
        in.write
          .format("parquet")
          .save("mmm")
      case "awsdbold" =>
        in.write
          .format("parquet")
          .save("testing12.3.com")
      case "emr" =>
        in.write
          .format("parquet")
          .mode("overwrite")
          .save("qwq")
      case "awsnewdp1" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",  IntegerType, false),
            StructField("country_code", StringType,  false),
            StructField("order_id",     LongType,    false),
            StructField("amount",       DoubleType,  false)
          )
        )
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("overwrite")
          .save("dbfs:/Prophecy/rajat@prophecy.io/CustomerOrdersDatasetOutput1.csv/")
      case "azdbdp1" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",  IntegerType, false),
            StructField("country_code", StringType,  false),
            StructField("order_id",     LongType,    false),
            StructField("amount",       DoubleType,  false)
          )
        )
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .save("dbfs:/Users/prophecy/eng/CustomerOrdersDatasetOutput7.csv/")
      case "livy" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",  IntegerType, false),
            StructField("country_code", StringType,  false),
            StructField("order_id",     LongType,    false),
            StructField("amount",       DoubleType,  false)
          )
        )
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("overwrite")
          .save("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomerOrdersDatasetOutput.csv/")
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
          .format("parquet")
          .option("compression", 1)
          .mode("overwrite")
          .save("dbfs:/Users/prophecy/eng/CustomerOrdersDatasetOutput6.csv")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
