package com.pipeline.routes

import com.pipeline.analytics.AnalysisAggregator
import com.pipeline.config.AppConfig
import com.pipeline.jobs.BatchJob
import com.pipeline.models.BatchJobRecord
import com.pipeline.registry.BatchRegistry
import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.apache.spark.sql.SparkSession
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class BatchRoutes(
    spark: SparkSession,
    config: AppConfig,
    registry: BatchRegistry
) extends LazyLogging {

  private val batchJob =
    new BatchJob(spark, registry, config.delta.batchBasePath)
  private val batchEc: ExecutionContext = ExecutionContext.fromExecutorService(
    java.util.concurrent.Executors
      .newFixedThreadPool(config.batch.threadPoolSize)
  )

  val routes: Route = pathPrefix("batch") {
    concat(
      // POST /batch/run
      path("run") {
        post {
          entity(as[JsValue]) { json =>
            val fields = json.asJsObject.fields

            val startDate = fields.get("startDate").map(_.convertTo[String])
            val endDate = fields.get("endDate").map(_.convertTo[String])
            val analysisType =
              fields.get("analysisType").map(_.convertTo[String])

            (startDate, endDate, analysisType) match {
              case (Some(sd), Some(ed), Some(at)) =>
                if (!AnalysisAggregator.validAnalysisTypes.contains(at)) {
                  complete(
                    StatusCodes.BadRequest -> JsObject(
                      "error" -> JsString("INVALID_REQUEST"),
                      "message" -> JsString(
                        s"Unknown analysisType: $at. Valid types: ${AnalysisAggregator.validAnalysisTypes.mkString(", ")}"
                      ),
                      "timestamp" -> JsString(Instant.now().toString)
                    )
                  )
                } else {
                  val batchId =
                    s"batch-${sd.replace("-", "")}-${UUID.randomUUID().toString.take(8)}"
                  val filters = fields.get("filters").map { f =>
                    f.asJsObject.fields.map { case (k, v) =>
                      k -> v.convertTo[String]
                    }
                  }

                  val record = BatchJobRecord(
                    batchId = batchId,
                    status = "PENDING",
                    analysisType = at,
                    sourcePath = config.batch.sourcePath,
                    startDate = sd,
                    endDate = ed,
                    filters = filters,
                    submitTime = Instant.now()
                  )
                  registry.register(record)

                  Future {
                    batchJob.run(
                      batchId,
                      config.batch.sourcePath,
                      sd,
                      ed,
                      filters,
                      at
                    )
                  }(batchEc)

                  complete(
                    StatusCodes.Accepted -> JsObject(
                      "batchId" -> JsString(batchId),
                      "status" -> JsString("PENDING")
                    )
                  )
                }

              case _ =>
                complete(
                  StatusCodes.BadRequest -> JsObject(
                    "error" -> JsString("INVALID_REQUEST"),
                    "message" -> JsString(
                      "Missing required fields: startDate, endDate, analysisType"
                    ),
                    "timestamp" -> JsString(Instant.now().toString)
                  )
                )
            }
          }
        }
      },

      // GET /batch/status/:batchId
      path("status" / Segment) { batchId =>
        get {
          registry.get(batchId) match {
            case Some(record) =>
              complete(StatusCodes.OK -> batchRecordToJson(record))
            case None =>
              complete(
                StatusCodes.NotFound -> JsObject(
                  "error" -> JsString("BATCH_NOT_FOUND"),
                  "message" -> JsString(
                    s"No batch job found with ID: $batchId"
                  ),
                  "timestamp" -> JsString(Instant.now().toString)
                )
              )
          }
        }
      },

      // GET /batch/data/:batchId/aggregated
      path("data" / Segment / "aggregated") { batchId =>
        get {
          parameters(
            "limit".as[Int].withDefault(config.batch.defaultPageSize),
            "offset".as[Int].withDefault(0)
          ) { (limit, offset) =>
            registry.get(batchId) match {
              case Some(record) if record.status == "COMPLETED" =>
                try {
                  val clampedLimit = math.min(limit, config.batch.maxPageSize)
                  val aggDf =
                    spark.read.format("delta").load(record.deltaAggPath.get)
                  val total = aggDf.count()
                  val page =
                    aggDf.toJSON.collect().drop(offset).take(clampedLimit)
                  val jsonRows = page.map(_.parseJson)

                  complete(
                    StatusCodes.OK -> JsObject(
                      "batchId" -> JsString(batchId),
                      "analysisType" -> JsString(record.analysisType),
                      "page" -> JsObject(
                        "limit" -> JsNumber(clampedLimit),
                        "offset" -> JsNumber(offset),
                        "returned" -> JsNumber(jsonRows.length),
                        "hasMore" -> JsBoolean(offset + clampedLimit < total)
                      ),
                      "data" -> JsArray(jsonRows.toVector)
                    )
                  )
                } catch {
                  case ex: Exception =>
                    logger.error(
                      s"Error reading aggregated data for batch $batchId",
                      ex
                    )
                    complete(
                      StatusCodes.InternalServerError -> JsObject(
                        "error" -> JsString("INTERNAL_ERROR"),
                        "message" -> JsString(ex.getMessage),
                        "timestamp" -> JsString(Instant.now().toString)
                      )
                    )
                }

              case Some(record) =>
                complete(
                  StatusCodes.Conflict -> JsObject(
                    "error" -> JsString("BATCH_NOT_READY"),
                    "message" -> JsString(
                      s"Batch not completed. Current status: ${record.status}"
                    ),
                    "status" -> JsString(record.status)
                  )
                )

              case None =>
                complete(
                  StatusCodes.NotFound -> JsObject(
                    "error" -> JsString("BATCH_NOT_FOUND"),
                    "message" -> JsString(
                      s"No batch job found with ID: $batchId"
                    ),
                    "timestamp" -> JsString(Instant.now().toString)
                  )
                )
            }
          }
        }
      },

      // GET /batch/data/:batchId
      path("data" / Segment) { batchId =>
        get {
          parameters(
            "limit".as[Int].withDefault(config.batch.defaultPageSize),
            "offset".as[Int].withDefault(0)
          ) { (limit, offset) =>
            registry.get(batchId) match {
              case Some(record) if record.status == "COMPLETED" =>
                try {
                  val clampedLimit = math.min(limit, config.batch.maxPageSize)
                  val rawDf =
                    spark.read.format("delta").load(record.deltaRawPath.get)
                  val total = rawDf.count()
                  val page =
                    rawDf.toJSON.collect().drop(offset).take(clampedLimit)
                  val jsonRows = page.map(_.parseJson)

                  complete(
                    StatusCodes.OK -> JsObject(
                      "batchId" -> JsString(batchId),
                      "status" -> JsString("COMPLETED"),
                      "rowCount" -> JsNumber(total),
                      "page" -> JsObject(
                        "limit" -> JsNumber(clampedLimit),
                        "offset" -> JsNumber(offset),
                        "returned" -> JsNumber(jsonRows.length),
                        "hasMore" -> JsBoolean(offset + clampedLimit < total)
                      ),
                      "data" -> JsArray(jsonRows.toVector)
                    )
                  )
                } catch {
                  case ex: Exception =>
                    logger.error(s"Error reading data for batch $batchId", ex)
                    complete(
                      StatusCodes.InternalServerError -> JsObject(
                        "error" -> JsString("INTERNAL_ERROR"),
                        "message" -> JsString(ex.getMessage),
                        "timestamp" -> JsString(Instant.now().toString)
                      )
                    )
                }

              case Some(record) =>
                complete(
                  StatusCodes.Conflict -> JsObject(
                    "error" -> JsString("BATCH_NOT_READY"),
                    "message" -> JsString(
                      s"Batch not completed. Current status: ${record.status}"
                    ),
                    "status" -> JsString(record.status)
                  )
                )

              case None =>
                complete(
                  StatusCodes.NotFound -> JsObject(
                    "error" -> JsString("BATCH_NOT_FOUND"),
                    "message" -> JsString(
                      s"No batch job found with ID: $batchId"
                    ),
                    "timestamp" -> JsString(Instant.now().toString)
                  )
                )
            }
          }
        }
      },

      // GET /batch/list
      path("list") {
        get {
          parameters("status".optional, "limit".as[Int].withDefault(20)) {
            (statusFilter, limit) =>
              val batches = statusFilter match {
                case Some(s) => registry.listByStatus(s)
                case None    => registry.listAll()
              }
              val limited = batches.take(limit)
              val batchJsons = limited.map(batchRecordToJson)

              complete(
                StatusCodes.OK -> JsObject(
                  "total" -> JsNumber(batches.size),
                  "returned" -> JsNumber(limited.size),
                  "batches" -> JsArray(batchJsons.toVector)
                )
              )
          }
        }
      }
    )
  }

  private def batchRecordToJson(record: BatchJobRecord): JsObject = {
    val fields = scala.collection.mutable.Map[String, JsValue](
      "batchId" -> JsString(record.batchId),
      "status" -> JsString(record.status),
      "analysisType" -> JsString(record.analysisType),
      "sourcePath" -> JsString(record.sourcePath),
      "startDate" -> JsString(record.startDate),
      "endDate" -> JsString(record.endDate),
      "submitTime" -> JsString(record.submitTime.toString)
    )

    record.filters.foreach(f =>
      fields += "filters" -> JsObject(f.map { case (k, v) => k -> JsString(v) })
    )
    record.startTime.foreach(t => fields += "startTime" -> JsString(t.toString))
    record.endTime.foreach(t => fields += "endTime" -> JsString(t.toString))
    record.rowCount.foreach(c => fields += "rowCount" -> JsNumber(c))
    record.deltaRawPath.foreach(p => fields += "deltaRawPath" -> JsString(p))
    record.deltaAggPath.foreach(p => fields += "deltaAggPath" -> JsString(p))
    record.errorMessage.foreach(m => fields += "errorMessage" -> JsString(m))

    JsObject(fields.toMap)
  }
}
