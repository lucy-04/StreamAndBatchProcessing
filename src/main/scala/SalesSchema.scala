import org.apache.spark.sql.types._

/**
 * Shared schema definition for sales data.
 * Used by both stream and batch pipelines to ensure consistency.
 */
object SalesSchema {
  val schema: StructType = new StructType()
    .add("product", StringType)
    .add("amount", DoubleType)
    .add("timestamp", DoubleType)
}
