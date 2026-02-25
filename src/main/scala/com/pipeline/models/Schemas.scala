package com.pipeline.models

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, functions => F}

object Schemas {

  val TransactionSchema: StructType = new StructType()
    .add("transaction_id", StringType, nullable = false)
    .add("event_timestamp", TimestampType, nullable = false)
    .add("processing_timestamp", TimestampType, nullable = true)
    .add("customer_id", StringType, nullable = false)
    .add("customer_tier", StringType, nullable = true)
    .add("product_id", StringType, nullable = false)
    .add("product_name", StringType, nullable = false)
    .add("category", StringType, nullable = false)
    .add("sub_category", StringType, nullable = true)
    .add("brand", StringType, nullable = true)
    .add("quantity", IntegerType, nullable = false)
    .add("unit_price", DoubleType, nullable = false)
    .add("discount_percent", DoubleType, nullable = true)
    .add("total_amount", DoubleType, nullable = false)
    .add("tax_amount", DoubleType, nullable = true)
    .add("currency", StringType, nullable = false)
    .add("payment_method", StringType, nullable = false)
    .add("card_network", StringType, nullable = true)
    .add("transaction_status", StringType, nullable = false)
    .add("channel", StringType, nullable = false)
    .add("store_id", StringType, nullable = true)
    .add("region", StringType, nullable = false)
    .add("city", StringType, nullable = false)
    .add("state", StringType, nullable = true)
    .add("postal_code", StringType, nullable = true)
    .add("device_type", StringType, nullable = true)
    .add("session_id", StringType, nullable = true)
    .add("ip_address", StringType, nullable = true)
    .add("is_fraudulent", BooleanType, nullable = true)
    .add("fraud_score", DoubleType, nullable = true)
    .add("batch_id", StringType, nullable = true)

  /** Parses raw Kafka DataFrame (with `value` column as bytes) into the
    * TransactionEvent schema. Used only by the streaming path.
    */
  def parseKafkaValue(df: DataFrame): DataFrame = {
    df.selectExpr("CAST(value AS STRING) AS json_value")
      .select(F.from_json(F.col("json_value"), TransactionSchema).alias("data"))
      .select("data.*")
  }
}
