package com.pipeline.registry

import com.pipeline.models.BatchJobRecord
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class BatchRegistry {

  private val registry = new ConcurrentHashMap[String, BatchJobRecord]()

  def register(record: BatchJobRecord): Unit = {
    registry.put(record.batchId, record)
  }

  def get(batchId: String): Option[BatchJobRecord] = {
    Option(registry.get(batchId))
  }

  def listAll(): Seq[BatchJobRecord] = {
    registry.values().asScala.toSeq
  }

  def listByStatus(status: String): Seq[BatchJobRecord] = {
    registry.values().asScala.filter(_.status == status).toSeq
  }

  def updateStatus(
      batchId: String,
      status: String,
      startTime: Option[Instant] = None,
      endTime: Option[Instant] = None,
      rowCount: Option[Long] = None,
      deltaRawPath: Option[String] = None,
      deltaAggPath: Option[String] = None,
      errorMessage: Option[String] = None
  ): Unit = {
    registry.compute(
      batchId,
      (_, existing) => {
        if (existing == null) null
        else
          existing.copy(
            status = status,
            startTime = startTime.orElse(existing.startTime),
            endTime = endTime.orElse(existing.endTime),
            rowCount = rowCount.orElse(existing.rowCount),
            deltaRawPath = deltaRawPath.orElse(existing.deltaRawPath),
            deltaAggPath = deltaAggPath.orElse(existing.deltaAggPath),
            errorMessage = errorMessage.orElse(existing.errorMessage)
          )
      }
    )
  }
}
