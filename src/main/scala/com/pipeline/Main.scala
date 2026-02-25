package com.pipeline

import com.pipeline.config.AppConfig
import com.pipeline.registry.BatchRegistry
import com.pipeline.routes.{BatchRoutes, ControlRoutes, StreamRoutes}
import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives._

import java.time.Instant
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

object Main extends App with LazyLogging {

  logger.info("=" * 60)
  logger.info("  Unified Transaction Pipeline — Starting")
  logger.info("=" * 60)

  // 1. Load configuration
  val config = AppConfig.load()
  logger.info(s"Configuration loaded: ${config.appName}")

  // 2. Initialize SparkSession
  val spark = SparkManager.init(config)
  logger.info(s"SparkSession ready. Master: ${config.spark.master}")

  // 3. Create BatchRegistry
  val registry = new BatchRegistry()

  // 4. Create Pekko Actor System
  implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "unified-pipeline")
  implicit val ec: ExecutionContext = system.executionContext

  val startTime = Instant.now()

  // 5. Wire all routes
  val streamRoutes = new StreamRoutes(spark, config)
  val batchRoutes = new BatchRoutes(spark, config, registry)
  val controlRoutes = new ControlRoutes(
    startTime,
    streamRoutes,
    () => {
      SparkManager.stop()
      system.terminate()
      System.exit(0)
    }
  )

  val allRoutes = concat(
    streamRoutes.routes,
    batchRoutes.routes,
    controlRoutes.routes
  )

  // 6. Bind HTTP server
  val bindingFuture =
    Http().newServerAt(config.http.host, config.http.port).bind(allRoutes)

  bindingFuture.foreach { binding =>
    logger.info(s"HTTP server bound to ${binding.localAddress}")
    logger.info(
      s"API available at http://${config.http.host}:${config.http.port}"
    )
    logger.info(s"Spark UI at ${spark.sparkContext.uiWebUrl.getOrElse("N/A")}")
    logger.info("=" * 60)
    logger.info("  Pipeline ready. Awaiting commands...")
    logger.info("=" * 60)
  }

  // 7. Register JVM shutdown hook
  sys.addShutdownHook {
    logger.info("JVM shutdown hook triggered. Cleaning up...")
    streamRoutes.stopAllQueries()
    SparkManager.stop()
    system.terminate()
  }

  // 8. Park the main thread — keeps the application alive
  Await.result(system.whenTerminated, Duration.Inf)
}
