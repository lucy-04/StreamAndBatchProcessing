import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Shared transformation logic for sales data.
 * Provides reusable aggregation functions used by both stream and batch pipelines.
 */
object SalesTransformations {

  /**
   * Aggregates sales by product, computing the total amount per product.
   * Works with both static DataFrames (batch) and streaming DataFrames.
   */
  def aggregateSales(df: DataFrame): DataFrame = {
    df.groupBy("product").agg(sum("amount").alias("total_sales"))
  }
}
