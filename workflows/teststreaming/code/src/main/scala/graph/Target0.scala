package graph

import org.apache.spark.sql.streaming.Trigger
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

@Visual(id = "Target0", label = "Target0", x = 686, y = 106, phase = 0)
object Target0 {

  @UsesDataset(id = "190", version = 0)
  def apply(spark: SparkSession, in: DataFrame): StreamingTarget = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "emr2" =>
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
        in.writeStream
          .format("kafka")
          .option("kafka.security.protocol", "SSL")
          .option("topic",                   "testcopy")
          .option("checkpointLocation",      "s3://abinitio-spark-redshift-testing/teststreaming-checkpoint")
          .option(
            "kafka.bootstrap.servers",
            "b-1.test-kafka-cluster-1.ufns57.c4.kafka.us-west-1.amazonaws.com:9094,b-2.test-kafka-cluster-1.ufns57.c4.kafka.us-west-1.amazonaws.com:9094"
          )
          .trigger(Trigger.Once())
          .start()
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
        in.writeStream
          .format("kafka")
          .option("kafka.security.protocol", "SSL")
          .option("topic",                   "testcopy")
          .option(
            "kafka.bootstrap.servers",
            "b-1.test-kafka-cluster-1.ufns57.c4.kafka.us-west-1.amazonaws.com:9094,b-2.test-kafka-cluster-1.ufns57.c4.kafka.us-west-1.amazonaws.com:9094"
          )
          .trigger(Trigger.Once())
          .start()
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
