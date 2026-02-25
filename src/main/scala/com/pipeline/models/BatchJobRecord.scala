package com.pipeline.models

import java.time.Instant

case class BatchJobRecord(
    batchId: String,
    status: String, // PENDING, RUNNING, COMPLETED, FAILED
    analysisType: String, // which AnalysisAggregator function to run
    sourcePath: String, // Delta Lake source table path
    startDate: String, // ISO date "2025-01-01" — inclusive lower bound
    endDate: String, // ISO date "2025-01-15" — inclusive upper bound
    filters: Option[
      Map[String, String]
    ], // Optional: {"category": "electronics", "region": "west"}
    submitTime: Instant,
    startTime: Option[Instant] = None,
    endTime: Option[Instant] = None,
    rowCount: Option[Long] = None,
    deltaRawPath: Option[String] = None,
    deltaAggPath: Option[String] = None,
    errorMessage: Option[String] = None
)
