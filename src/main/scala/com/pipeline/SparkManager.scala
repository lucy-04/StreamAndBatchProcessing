package com.pipeline

import com.pipeline.config.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object SparkManager extends LazyLogging {

  @volatile private var _spark: SparkSession = _

  def init(config: AppConfig): SparkSession = synchronized {
    if (_spark == null || _spark.sparkContext.isStopped) {
      logger.info(
        "Initializing SparkSession with FAIR scheduler and Delta Lake extensions..."
      )

      _spark = SparkSession
        .builder()
        .appName(config.appName)
        .master(config.spark.master)
        // FAIR Scheduler
        .config("spark.scheduler.mode", "FAIR")
        .config(
          "spark.scheduler.allocation.file",
          config.spark.fairSchedulerFile
        )
        // Delta Lake
        .config(
          "spark.sql.extensions",
          "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        // Streaming
        .config(
          "spark.sql.streaming.checkpointLocation",
          config.spark.checkpointDir
        )
        .config("spark.sql.streaming.minBatchesToRetain", "100")
        // Memory
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()

      logger.info(
        s"SparkSession initialized. Spark UI: ${_spark.sparkContext.uiWebUrl.getOrElse("N/A")}"
      )
    }
    _spark
  }

  def spark: SparkSession = {
    require(
      _spark != null && !_spark.sparkContext.isStopped,
      "SparkSession not initialized. Call SparkManager.init(config) first."
    )
    _spark
  }

  def isHealthy: Boolean = {
    _spark != null && !_spark.sparkContext.isStopped
  }

  def stop(): Unit = synchronized {
    if (_spark != null && !_spark.sparkContext.isStopped) {
      logger.info("Stopping SparkSession...")
      _spark.stop()
      logger.info("SparkSession stopped.")
    }
  }
}
