package com.pipeline.routes

import com.pipeline.config.AppConfig
import com.pipeline.jobs.StreamingJob
import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.util.concurrent.atomic.AtomicReference

class StreamRoutes(spark: SparkSession, config: AppConfig) extends LazyLogging {

  private val activeQueries =
    new AtomicReference[Seq[StreamingQuery]](Seq.empty)
  private val streamingJob = new StreamingJob(spark, config)

  val routes: Route = pathPrefix("stream") {
    concat(
      // POST /stream/start
      path("start") {
        post {
          entity(as[JsValue]) { json =>
            val topic = json.asJsObject.fields
              .get("topic")
              .map(_.convertTo[String])
              .getOrElse(config.kafka.streamTopic)

            try {
              val currentQueries = activeQueries.get()
              if (
                currentQueries.nonEmpty && currentQueries.exists(_.isActive)
              ) {
                complete(
                  StatusCodes.Conflict -> JsObject(
                    "error" -> JsString("STREAM_ALREADY_RUNNING"),
                    "message" -> JsString(
                      "Streaming queries are already active. Stop them first."
                    )
                  )
                )
              } else {
                val queries = streamingJob.start(topic)
                activeQueries.set(queries)
                val queryIds = queries.map(q => JsString(q.id.toString))
                complete(
                  StatusCodes.OK -> JsObject(
                    "status" -> JsString("started"),
                    "queryIds" -> JsArray(queryIds.toVector)
                  )
                )
              }
            } catch {
              case ex: Exception =>
                logger.error("Failed to start streaming", ex)
                complete(
                  StatusCodes.InternalServerError -> JsObject(
                    "error" -> JsString("INTERNAL_ERROR"),
                    "message" -> JsString(ex.getMessage)
                  )
                )
            }
          }
        }
      },

      // POST /stream/stop
      path("stop") {
        post {
          val queries = activeQueries.get()
          if (queries.isEmpty) {
            complete(
              StatusCodes.OK -> JsObject(
                "status" -> JsString("stopped"),
                "queriesStopped" -> JsNumber(0),
                "message" -> JsString("No active streaming queries.")
              )
            )
          } else {
            var stopped = 0
            queries.foreach { q =>
              if (q.isActive) {
                q.stop()
                stopped += 1
              }
            }
            activeQueries.set(Seq.empty)
            logger.info(s"Stopped $stopped streaming queries.")
            complete(
              StatusCodes.OK -> JsObject(
                "status" -> JsString("stopped"),
                "queriesStopped" -> JsNumber(stopped)
              )
            )
          }
        }
      },

      // GET /stream/status
      path("status") {
        get {
          val queries = activeQueries.get()
          val active = queries.exists(_.isActive)
          val queryDetails = queries.map { q =>
            JsObject(
              "id" -> JsString(q.id.toString),
              "name" -> JsString(Option(q.name).getOrElse("unnamed")),
              "isActive" -> JsBoolean(q.isActive)
            )
          }
          complete(
            StatusCodes.OK -> JsObject(
              "active" -> JsBoolean(active),
              "queries" -> JsArray(queryDetails.toVector)
            )
          )
        }
      }
    )
  }

  def stopAllQueries(): Int = {
    val queries = activeQueries.get()
    var stopped = 0
    queries.foreach { q =>
      if (q.isActive) {
        q.stop()
        stopped += 1
      }
    }
    activeQueries.set(Seq.empty)
    stopped
  }
}
