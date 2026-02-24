import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Unified processor that runs both batch and stream processing
 * within a single SparkSession, sharing schema and transformation logic.
 *
 * Flow:
 *   1. Batch: Read CSV -> apply shared schema -> aggregate -> print results
 *   2. Stream: Read Kafka -> parse JSON with shared schema -> aggregate -> console output
 */
object UnifiedProcessor {
  def main(args: Array[String]): Unit = {

    // ── Single SparkSession for both pipelines ──
    val spark = SparkSession.builder
      .appName("UnifiedSalesProcessor")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // ═══════════════════════════════════════════
    //  BATCH PIPELINE
    // ═══════════════════════════════════════════
    println("\n=== Batch Processing Results ===")

    val batchDf = spark.read
      .option("header", "true")
      .schema(SalesSchema.schema)  // shared schema
      .csv("sales_data.csv")

    val batchReport = SalesTransformations.aggregateSales(batchDf)  // shared transformation
    batchReport.show()

    // ═══════════════════════════════════════════
    //  STREAM PIPELINE
    // ═══════════════════════════════════════════
    println("\n=== Starting Stream Processing ===")

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sales_stream")
      .load()

    val streamDf = rawStream
      .select(from_json($"value".cast("string"), SalesSchema.schema).as("data"))  // shared schema
      .select("data.*")

    val streamReport = SalesTransformations.aggregateSales(streamDf)  // shared transformation

    val query = streamReport.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
