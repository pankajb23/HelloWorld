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

@Visual(id = "CustomersDatasetInput", label = "CustomersDatasetInput", x = 71, y = 210, phase = 0)
object CustomersDatasetInput {

  @UsesDataset(id = "15", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val out = Config.fabricName match {
      case "awsdp2" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",       IntegerType,   true),
            StructField("first_name",        StringType,    true),
            StructField("last_name",         StringType,    true),
            StructField("phone",             StringType,    true),
            StructField("email",             StringType,    true),
            StructField("country_code",      StringType,    true),
            StructField("account_open_date", TimestampType, true),
            StructField("account_flags",     StringType,    true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("dbfs:/Prophecy/rajat@prophecy.io/CustomersDatasetInput.csv")
          .cache()
      case "emr2" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",       IntegerType,   true),
            StructField("first_name",        StringType,    true),
            StructField("last_name",         StringType,    true),
            StructField("phone",             StringType,    true),
            StructField("email",             StringType,    true),
            StructField("country_code",      StringType,    true),
            StructField("account_open_date", TimestampType, true),
            StructField("account_flags",     StringType,    true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomersDatasetInput.csv")
          .cache()
      case "narayan" =>
        spark.read
          .format("parquet")
          .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomerOrdersDatasetOutput.csv/")
          .cache()
      case "awsnewdp1" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",       IntegerType,   true),
            StructField("first_name",        StringType,    true),
            StructField("last_name",         StringType,    true),
            StructField("phone",             StringType,    true),
            StructField("email",             StringType,    true),
            StructField("country_code",      StringType,    true),
            StructField("account_open_date", TimestampType, true),
            StructField("account_flags",     StringType,    true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("dbfs:/Prophecy/rajat@prophecy.io/CustomersDatasetInput.csv")
          .cache()
      case "azdbdp1" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",       IntegerType,   true),
            StructField("first_name",        StringType,    true),
            StructField("last_name",         StringType,    true),
            StructField("phone",             StringType,    true),
            StructField("email",             StringType,    true),
            StructField("country_code",      StringType,    true),
            StructField("account_open_date", TimestampType, true),
            StructField("account_flags",     StringType,    true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("dbfs:/Users/prophecy/eng/CustomersDatasetInput.csv")
          .cache()
      case "livy" =>
        spark.read
          .format("csv")
          .option("header",      true)
          .option("sep",         ",")
          .option("inferSchema", true)
          .load("s3://abinitio-spark-redshift-testing/Users/prophecy/eng/CustomersDatasetInput.csv")
          .cache()
      case "azdb" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",       IntegerType, false),
            StructField("first_name",        StringType,  false),
            StructField("last_name",         StringType,  false),
            StructField("phone",             StringType,  false),
            StructField("email",             StringType,  false),
            StructField("country_code",      StringType,  false),
            StructField("account_open_date", StringType,  false),
            StructField("account_flags",     StringType,  false)
          )
        )
        spark.read
          .format("csv")
          .option("sep", ",")
          .schema(schemaArg)
          .load("dbfs:/Users/prophecy/eng/CustomersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric is not handled")
    }

    out

  }

}
