name := "UnifiedTransactionPipeline"
version := "1.0.0"
scalaVersion := "2.13.13"

val sparkVersion = "3.5.1"
val pekkoVersion = "1.0.2"
val pekkoHttpVersion = "1.0.1"
val deltaVersion = "3.1.0"
val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // Delta Lake
  "io.delta" %% "delta-spark" % deltaVersion,

  // Pekko HTTP Server
  "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
  "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHttpVersion,
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-stream" % pekkoVersion,

  // JSON
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,

  // Config
  "com.typesafe" % "config" % "1.4.3",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpVersion % Test
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}
