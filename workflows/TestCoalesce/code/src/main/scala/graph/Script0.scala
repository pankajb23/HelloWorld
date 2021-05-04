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
import udfs._
import graph._

@Visual(id = "Script0", label = "Script0", x = 366, y = 25, phase = 0)
object Script0 {

  def apply(spark: SparkSession): ScriptUnit = {
    import spark.implicits._

    spark.conf.set(
      "spark.sql.optimizer.excludedRules",
      "org.apache.spark.sql.catalyst.optimizer.PushProjectionThroughUnion,org.apache.spark.sql.catalyst.optimizer.ReorderJoin,org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin,org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughJoin,org.apache.spark.sql.catalyst.optimizer.PushDownPredicate,org.apache.spark.sql.catalyst.optimizer.LimitPushDown,org.apache.spark.sql.catalyst.optimizer.ColumnPruning,org.apache.spark.sql.catalyst.optimizer.ColumnPruning,org.apache.spark.sql.catalyst.optimizer.CollapseProject,org.apache.spark.sql.catalyst.optimizer.CollapseWindow,org.apache.spark.sql.catalyst.optimizer.CombineFilters,org.apache.spark.sql.catalyst.optimizer.CombineLimits,org.apache.spark.sql.catalyst.optimizer.CombineUnions,org.apache.spark.sql.catalyst.optimizer.NullPropagation,org.apache.spark.sql.catalyst.optimizer.ConstantPropagation,org.apache.spark.sql.catalyst.optimizer.FoldablePropagation,org.apache.spark.sql.catalyst.optimizer.OptimizeIn,org.apache.spark.sql.catalyst.optimizer.ConstantFolding,org.apache.spark.sql.catalyst.optimizer.ReorderAssociativeOperator,org.apache.spark.sql.catalyst.optimizer.LikeSimplification,org.apache.spark.sql.catalyst.optimizer.BooleanSimplification,org.apache.spark.sql.catalyst.optimizer.SimplifyConditionals,org.apache.spark.sql.catalyst.optimizer.RemoveDispensableExpressions,org.apache.spark.sql.catalyst.optimizer.SimplifyBinaryComparison,org.apache.spark.sql.catalyst.optimizer.PruneFilters,org.apache.spark.sql.catalyst.optimizer.EliminateSorts,org.apache.spark.sql.catalyst.optimizer.SimplifyCasts,org.apache.spark.sql.catalyst.optimizer.SimplifyCaseConversionExpressions,org.apache.spark.sql.catalyst.optimizer.RewriteCorrelatedScalarSubquery,org.apache.spark.sql.catalyst.optimizer.EliminateSerialization,org.apache.spark.sql.catalyst.optimizer.RemoveRedundantAliases,org.apache.spark.sql.catalyst.optimizer.RemoveRedundantProject,org.apache.spark.sql.catalyst.optimizer.SimplifyExtractValueOps,org.apache.spark.sql.catalyst.optimizer.CombineConcats"
    )

  }

}
