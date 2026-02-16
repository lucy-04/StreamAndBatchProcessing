name := "SparkKafkaDemo"
version := "1.0"
scalaVersion := "2.13.13" // Spark 3.5 works best with 2.12 or 2.13

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0" // CRITICAL for Kafka
)