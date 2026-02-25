package com.pipeline.config

import com.typesafe.config.{Config, ConfigFactory}

case class HttpConfig(host: String, port: Int)

case class SparkConfig(
    master: String,
    checkpointDir: String,
    fairSchedulerFile: String
)

case class KafkaConfig(
    bootstrapServers: String,
    streamTopic: String,
    consumerGroupPrefix: String
)

case class DeltaConfig(
    basePath: String,
    streamTransactionsPath: String,
    streamAggregationsPath: String,
    batchBasePath: String,
    seedPath: String
)

case class BatchConfig(
    threadPoolSize: Int,
    defaultPageSize: Int,
    maxPageSize: Int,
    sourcePath: String
)

case class AppConfig(
    appName: String,
    http: HttpConfig,
    spark: SparkConfig,
    kafka: KafkaConfig,
    delta: DeltaConfig,
    batch: BatchConfig
)

object AppConfig {
  def load(): AppConfig = {
    val config: Config = ConfigFactory.load()
    val pipeline = config.getConfig("pipeline")

    AppConfig(
      appName = pipeline.getString("app-name"),
      http = HttpConfig(
        host = pipeline.getString("http.host"),
        port = pipeline.getInt("http.port")
      ),
      spark = SparkConfig(
        master = pipeline.getString("spark.master"),
        checkpointDir = pipeline.getString("spark.checkpoint-dir"),
        fairSchedulerFile = pipeline.getString("spark.fair-scheduler-file")
      ),
      kafka = KafkaConfig(
        bootstrapServers = pipeline.getString("kafka.bootstrap-servers"),
        streamTopic = pipeline.getString("kafka.stream-topic"),
        consumerGroupPrefix = pipeline.getString("kafka.consumer-group-prefix")
      ),
      delta = DeltaConfig(
        basePath = pipeline.getString("delta.base-path"),
        streamTransactionsPath =
          pipeline.getString("delta.stream-transactions-path"),
        streamAggregationsPath =
          pipeline.getString("delta.stream-aggregations-path"),
        batchBasePath = pipeline.getString("delta.batch-base-path"),
        seedPath = pipeline.getString("delta.seed-path")
      ),
      batch = BatchConfig(
        threadPoolSize = pipeline.getInt("batch.thread-pool-size"),
        defaultPageSize = pipeline.getInt("batch.default-page-size"),
        maxPageSize = pipeline.getInt("batch.max-page-size"),
        sourcePath = pipeline.getString("batch.source-path")
      )
    )
  }
}
