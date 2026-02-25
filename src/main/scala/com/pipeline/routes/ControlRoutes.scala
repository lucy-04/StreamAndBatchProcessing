package com.pipeline.routes

import com.pipeline.SparkManager
import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.time.{Duration, Instant}

class ControlRoutes(
    startTime: Instant,
    streamRoutes: StreamRoutes,
    onStop: () => Unit
) extends LazyLogging {

  val routes: Route = concat(
    // GET /health
    path("health") {
      get {
        val healthy = SparkManager.isHealthy
        val uptime = Duration.between(startTime, Instant.now())
        val uptimeStr =
          s"${uptime.toHours}h ${uptime.toMinutesPart}m ${uptime.toSecondsPart}s"

        if (healthy) {
          complete(
            StatusCodes.OK -> JsObject(
              "status" -> JsString("healthy"),
              "spark" -> JsBoolean(true),
              "uptime" -> JsString(uptimeStr)
            )
          )
        } else {
          complete(
            StatusCodes.ServiceUnavailable -> JsObject(
              "status" -> JsString("unhealthy"),
              "spark" -> JsBoolean(false),
              "uptime" -> JsString(uptimeStr)
            )
          )
        }
      }
    },

    // POST /stop
    path("stop") {
      post {
        logger.info("Received shutdown request. Initiating graceful drain...")

        // Stop streaming queries first
        val queriesStopped = streamRoutes.stopAllQueries()
        logger.info(s"Stopped $queriesStopped streaming queries.")

        // Schedule shutdown in a separate thread to allow HTTP response
        new Thread(
          () => {
            Thread.sleep(2000) // Give time for HTTP response
            logger.info("Draining in-flight batch jobs (60s timeout)...")
            Thread.sleep(5000) // Brief wait for in-flight batches
            onStop()
          },
          "shutdown-thread"
        ).start()

        complete(
          StatusCodes.OK -> JsObject(
            "status" -> JsString("shutting_down"),
            "queriesStopped" -> JsNumber(queriesStopped)
          )
        )
      }
    }
  )
}
