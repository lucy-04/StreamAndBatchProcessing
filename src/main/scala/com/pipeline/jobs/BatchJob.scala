package com.pipeline.jobs

import com.pipeline.analytics.AnalysisAggregator
import com.pipeline.registry.BatchRegistry
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.Instant

class BatchJob(
    spark: SparkSession,
    registry: BatchRegistry,
    batchBasePath: String
) extends LazyLogging {

  /** Executes a batch analysis job:
    *   1. Reads source data from the streaming Delta table (filtered by date
    *      range + optional filters)
    *   2. Writes a snapshot to delta://batches/{batchId}/raw/
    *   3. Runs the requested AnalysisAggregator function
    *   4. Writes aggregated results to delta://batches/{batchId}/aggregated/
    *   5. Updates the BatchRegistry with COMPLETED or FAILED status
    */
  def run(
      batchId: String,
      sourcePath: String,
      startDate: String,
      endDate: String,
      filters: Option[Map[String, String]],
      analysisType: String
  ): Unit = {
    try {
      // Set scheduler pool for batch
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch-pool")

      // Update status to RUNNING
      registry.updateStatus(batchId, "RUNNING", startTime = Some(Instant.now()))
      logger.info(
        s"Batch job $batchId started: $analysisType ($startDate to $endDate)"
      )

      // Read from the streaming Delta table with partition pruning
      var sourceDf = spark.read
        .format("delta")
        .load(sourcePath)
        .filter(col("event_date").between(startDate, endDate))

      // Apply optional filters
      filters.foreach { filterMap =>
        filterMap.foreach { case (columnName, value) =>
          sourceDf = sourceDf.filter(col(columnName) === value)
        }
      }

      // Add batch metadata
      sourceDf = sourceDf
        .withColumn("batch_id", lit(batchId))
        .withColumn("processing_timestamp", current_timestamp())

      val rawPath = s"$batchBasePath/$batchId/raw"
      val aggPath = s"$batchBasePath/$batchId/aggregated"

      // Write filtered source snapshot to Delta (for audit/reproducibility)
      sourceDf.write
        .format("delta")
        .partitionBy("category")
        .mode("overwrite")
        .save(rawPath)

      val rowCount = sourceDf.count()
      logger.info(s"Batch job $batchId: wrote $rowCount rows to $rawPath")

      // Run analysis aggregation
      val analysisResults =
        AnalysisAggregator.runAnalysis(sourceDf, spark, analysisType)

      // Write aggregated results
      analysisResults.foreach { case (name, resultDf) =>
        val outputPath =
          if (analysisResults.size == 1) aggPath
          else s"$aggPath/$name"
        resultDf.write
          .format("delta")
          .mode("overwrite")
          .save(outputPath)
        logger.info(
          s"Batch job $batchId: wrote aggregation '$name' to $outputPath"
        )
      }

      // Update registry with COMPLETED
      registry.updateStatus(
        batchId,
        "COMPLETED",
        endTime = Some(Instant.now()),
        rowCount = Some(rowCount),
        deltaRawPath = Some(rawPath),
        deltaAggPath = Some(aggPath)
      )
      logger.info(s"Batch job $batchId completed successfully.")

    } catch {
      case ex: Exception =>
        logger.error(s"Batch job $batchId failed: ${ex.getMessage}", ex)
        registry.updateStatus(
          batchId,
          "FAILED",
          endTime = Some(Instant.now()),
          errorMessage = Some(ex.getMessage)
        )
    } finally {
      // Clear the local property
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
    }
  }
}
