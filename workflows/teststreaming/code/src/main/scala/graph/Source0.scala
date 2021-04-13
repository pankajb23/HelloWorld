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

@Visual(id = "Source0", label = "Source0", x = 31, y = 75, phase = 0)
object Source0 {

  @UsesDataset(id = "189", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "emr2" =>
        spark.readStream
          .format("kafka")
          .option("kafka.security.protocol", "SSL")
          .option("startingOffsets",         "earliest")
          .option(
            "kafka.bootstrap.servers",
            "b-1.test-kafka-cluster-1.ufns57.c4.kafka.us-west-1.amazonaws.com:9094,b-2.test-kafka-cluster-1.ufns57.c4.kafka.us-west-1.amazonaws.com:9094"
          )
          .option("subscribe", "test")
          .load()
      case "azdb" =>
        val schemaArg = StructType(
          Array(
            StructField("key",           BinaryType,    false),
            StructField("value",         BinaryType,    false),
            StructField("topic",         StringType,    false),
            StructField("partition",     IntegerType,   false),
            StructField("offset",        LongType,      false),
            StructField("timestamp",     TimestampType, false),
            StructField("timestampType", IntegerType,   false)
          )
        )
        spark.readStream
          .format("kafka")
          .option("kafka.security.protocol", "SSL")
          .option("startingOffsets",         "earliest")
          .option(
            "kafka.bootstrap.servers",
            "b-1.test-kafka-cluster-1.ufns57.c4.kafka.us-west-1.amazonaws.com:9094,b-2.test-kafka-cluster-1.ufns57.c4.kafka.us-west-1.amazonaws.com:9094"
          )
          .option("subscribe", "test")
          .load()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
