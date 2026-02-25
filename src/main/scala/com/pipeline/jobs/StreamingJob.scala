package com.pipeline.jobs

import com.pipeline.config.AppConfig
import com.pipeline.models.Schemas
import com.pipeline.analytics.AnalysisAggregator
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

class StreamingJob(spark: SparkSession, config: AppConfig) extends LazyLogging {

  /** Starts the Kafka streaming pipeline:
    *   1. Raw events: Kafka → Delta Lake (stream/transactions)
    *   2. Real-time aggregations: windowed metrics → Delta Lake
    *      (stream/stream_aggregations)
    *
    * Returns the streaming query handles for lifecycle management.
    */
  def start(topic: String): Seq[StreamingQuery] = {
    logger.info(s"Starting streaming pipeline for topic: $topic")

    // Set scheduler pool for streaming
    spark.sparkContext.setLocalProperty(
      "spark.scheduler.pool",
      "streaming-pool"
    )

    // Read from Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // Parse JSON using the shared schema
    val parsedStream = Schemas
      .parseKafkaValue(kafkaStream)
      .withColumn("event_date", to_date(col("event_timestamp")))
      .withColumn("processing_timestamp", current_timestamp())

    // Query 1: Write raw events to Delta Lake
    val rawQuery = parsedStream.writeStream
      .format("delta")
      .partitionBy("event_date")
      .outputMode("append")
      .option(
        "checkpointLocation",
        s"${config.spark.checkpointDir}/stream/transactions"
      )
      .start(config.delta.streamTransactionsPath)

    logger.info(s"Raw streaming query started: ${rawQuery.id}")

    // Query 2: Real-time aggregations
    val aggDf = AnalysisAggregator.realtimeDashboard(parsedStream, spark)

    val aggQuery = aggDf.writeStream
      .format("delta")
      .outputMode("complete")
      .option(
        "checkpointLocation",
        s"${config.spark.checkpointDir}/stream/aggregations"
      )
      .start(config.delta.streamAggregationsPath)

    logger.info(s"Aggregation streaming query started: ${aggQuery.id}")

    Seq(rawQuery, aggQuery)
  }
}
