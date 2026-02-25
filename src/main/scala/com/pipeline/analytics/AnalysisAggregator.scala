package com.pipeline.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.typesafe.scalalogging.LazyLogging

object AnalysisAggregator extends LazyLogging {

  /** Dispatches to the correct analysis function based on the analysisType
    * string. For "full_report", runs all analysis types and returns a map of
    * results.
    */
  def runAnalysis(
      df: DataFrame,
      spark: SparkSession,
      analysisType: String
  ): Map[String, DataFrame] = {
    analysisType match {
      case "revenue_by_category" =>
        Map("revenue_by_category" -> revenueByCategory(df, spark))
      case "revenue_by_region" =>
        Map("revenue_by_region" -> revenueByRegionAndCity(df, spark))
      case "payment_analysis" =>
        Map("payment_analysis" -> paymentMethodAnalysis(df, spark))
      case "customer_segmentation" =>
        Map("customer_segmentation" -> customerSegmentation(df, spark))
      case "fraud_analysis" =>
        Map("fraud_analysis" -> fraudDetectionAnalysis(df, spark))
      case "hourly_trends" =>
        Map("hourly_trends" -> hourlyTrendAnalysis(df, spark))
      case "channel_performance" =>
        Map("channel_performance" -> channelPerformance(df, spark))
      case "inventory_velocity" =>
        Map("inventory_velocity" -> inventoryVelocity(df, spark))
      case "full_report" =>
        Map(
          "revenue_by_category" -> revenueByCategory(df, spark),
          "revenue_by_region" -> revenueByRegionAndCity(df, spark),
          "payment_analysis" -> paymentMethodAnalysis(df, spark),
          "customer_segmentation" -> customerSegmentation(df, spark),
          "fraud_analysis" -> fraudDetectionAnalysis(df, spark),
          "hourly_trends" -> hourlyTrendAnalysis(df, spark),
          "channel_performance" -> channelPerformance(df, spark),
          "inventory_velocity" -> inventoryVelocity(df, spark)
        )
      case other =>
        throw new IllegalArgumentException(s"Unknown analysisType: $other")
    }
  }

  val validAnalysisTypes: Set[String] = Set(
    "revenue_by_category",
    "revenue_by_region",
    "payment_analysis",
    "customer_segmentation",
    "fraud_analysis",
    "hourly_trends",
    "channel_performance",
    "inventory_velocity",
    "full_report"
  )

  // ──────────────────────────────────────────
  // 7.1 Revenue by Category
  // ──────────────────────────────────────────

  def revenueByCategory(df: DataFrame, spark: SparkSession): DataFrame = {
    df.filter(col("transaction_status") === "completed")
      .groupBy("category")
      .agg(
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value"),
        count("transaction_id").alias("transaction_count"),
        sum("quantity").alias("total_units_sold"),
        avg("discount_percent").alias("avg_discount_pct"),
        countDistinct("customer_id").alias("unique_customers")
      )
      .orderBy(desc("total_revenue"))
  }

  // ──────────────────────────────────────────
  // 7.2 Revenue by Region and City
  // ──────────────────────────────────────────

  def revenueByRegionAndCity(df: DataFrame, spark: SparkSession): DataFrame = {
    df.filter(col("transaction_status") === "completed")
      .groupBy("region", "city")
      .agg(
        sum("total_amount").alias("total_revenue"),
        count("transaction_id").alias("transaction_count"),
        avg("total_amount").alias("avg_order_value"),
        countDistinct("store_id").alias("active_stores"),
        countDistinct("customer_id").alias("unique_customers")
      )
      .orderBy(desc("total_revenue"))
  }

  // ──────────────────────────────────────────
  // 7.3 Payment Method Analysis
  // ──────────────────────────────────────────

  def paymentMethodAnalysis(df: DataFrame, spark: SparkSession): DataFrame = {
    df.filter(col("transaction_status") === "completed")
      .groupBy("payment_method", "card_network")
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_transaction_value"),
        countDistinct("customer_id").alias("unique_customers")
      )
      .withColumn(
        "revenue_share_pct",
        round(
          col("total_revenue") / sum("total_revenue")
            .over(Window.partitionBy()) * 100,
          2
        )
      )
      .orderBy(desc("transaction_count"))
  }

  // ──────────────────────────────────────────
  // 7.4 Customer Segmentation
  // ──────────────────────────────────────────

  def customerSegmentation(df: DataFrame, spark: SparkSession): DataFrame = {
    val customerMetrics = df
      .filter(col("transaction_status") === "completed")
      .groupBy("customer_id", "customer_tier")
      .agg(
        sum("total_amount").alias("lifetime_value"),
        count("transaction_id").alias("total_transactions"),
        avg("total_amount").alias("avg_order_value"),
        max("event_timestamp").alias("last_purchase_date"),
        min("event_timestamp").alias("first_purchase_date"),
        countDistinct("category").alias("category_diversity"),
        sum(when(col("channel") === "mobile_app", 1).otherwise(0))
          .alias("mobile_orders"),
        sum(when(col("channel") === "web", 1).otherwise(0)).alias("web_orders"),
        sum(when(col("channel") === "pos_in_store", 1).otherwise(0))
          .alias("instore_orders")
      )

    customerMetrics
      .withColumn(
        "segment",
        when(col("lifetime_value") >= 10000, "vip")
          .when(col("lifetime_value") >= 5000, "high_value")
          .when(col("lifetime_value") >= 1000, "mid_value")
          .when(col("total_transactions") === 1, "one_time")
          .otherwise("low_value")
      )
      .orderBy(desc("lifetime_value"))
  }

  // ──────────────────────────────────────────
  // 7.5 Fraud Detection Analysis
  // ──────────────────────────────────────────

  def fraudDetectionAnalysis(df: DataFrame, spark: SparkSession): DataFrame = {
    val fraudDf =
      df.filter(col("is_fraudulent") === true || col("fraud_score") > 0.7)

    fraudDf
      .groupBy("category", "region", "channel", "payment_method")
      .agg(
        count("transaction_id").alias("fraud_count"),
        sum("total_amount").alias("fraud_total_amount"),
        avg("fraud_score").alias("avg_fraud_score"),
        avg("total_amount").alias("avg_fraud_amount"),
        countDistinct("customer_id").alias("affected_customers"),
        collect_set("city").alias("affected_cities")
      )
      .orderBy(desc("fraud_count"))
  }

  // ──────────────────────────────────────────
  // 7.6 Hourly Trend Analysis
  // ──────────────────────────────────────────

  def hourlyTrendAnalysis(df: DataFrame, spark: SparkSession): DataFrame = {
    df.filter(col("transaction_status") === "completed")
      .withColumn("event_date", to_date(col("event_timestamp")))
      .withColumn("event_hour", hour(col("event_timestamp")))
      .groupBy("event_date", "event_hour")
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("total_amount").alias("hourly_revenue"),
        avg("total_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("units_sold")
      )
      .orderBy("event_date", "event_hour")
  }

  // ──────────────────────────────────────────
  // 7.7 Channel Performance
  // ──────────────────────────────────────────

  def channelPerformance(df: DataFrame, spark: SparkSession): DataFrame = {
    df.filter(col("transaction_status") === "completed")
      .groupBy("channel")
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("discount_percent").alias("avg_discount"),
        sum(when(col("is_fraudulent") === true, 1).otherwise(0))
          .alias("fraud_count"),
        avg("fraud_score").alias("avg_fraud_score")
      )
      .withColumn(
        "fraud_rate_pct",
        round(col("fraud_count") / col("transaction_count") * 100, 4)
      )
      .orderBy(desc("total_revenue"))
  }

  // ──────────────────────────────────────────
  // 7.8 Real-time Dashboard (Streaming-Specific)
  // ──────────────────────────────────────────

  def realtimeDashboard(df: DataFrame, spark: SparkSession): DataFrame = {
    df.filter(col("transaction_status").isin("completed", "pending"))
      .withWatermark("event_timestamp", "10 minutes")
      .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute"),
        col("category")
      )
      .agg(
        count("transaction_id").alias("txn_count"),
        sum("total_amount").alias("window_revenue"),
        avg("total_amount").alias("avg_order_value"),
        sum(when(col("is_fraudulent") === true, 1).otherwise(0))
          .alias("fraud_alerts"),
        approx_count_distinct("customer_id").alias("approx_unique_customers")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("txn_count"),
        col("window_revenue"),
        col("avg_order_value"),
        col("fraud_alerts"),
        col("approx_unique_customers")
      )
  }

  // ──────────────────────────────────────────
  // 7.9 Inventory Velocity
  // ──────────────────────────────────────────

  def inventoryVelocity(df: DataFrame, spark: SparkSession): DataFrame = {
    df.filter(col("transaction_status") === "completed")
      .groupBy(
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "brand"
      )
      .agg(
        sum("quantity").alias("total_units_sold"),
        count("transaction_id").alias("order_count"),
        sum("total_amount").alias("total_revenue"),
        avg("unit_price").alias("avg_selling_price"),
        avg("discount_percent").alias("avg_discount"),
        countDistinct("region").alias("regions_sold_in"),
        countDistinct("store_id").alias("stores_sold_in"),
        min("event_timestamp").alias("first_sale"),
        max("event_timestamp").alias("last_sale")
      )
      .withColumn(
        "days_in_range",
        datediff(col("last_sale"), col("first_sale")) + 1
      )
      .withColumn(
        "daily_velocity",
        round(col("total_units_sold") / col("days_in_range"), 2)
      )
      .orderBy(desc("daily_velocity"))
  }
}
