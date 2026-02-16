import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("StreamProcessor")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 1. Read Stream from Kafka
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sales_stream")
      .load()

    // 2. Parse JSON Data
    val schema = new StructType()
      .add("product", StringType)
      .add("amount", DoubleType)
      .add("timestamp", DoubleType)

    val salesData = df.select(from_json($"value".cast("string"), schema).as("data"))
      .select("data.*")

    // 3. Process: Calculate Running Total Sales per Product
    val runningCounts = salesData.groupBy("product").sum("amount")

    // 4. Write Stream to Console
    val query = runningCounts.writeStream
      .outputMode("complete") // "complete" for aggregations, "append" for simple lists
      .format("console")
      .start()

    query.awaitTermination()
  }
}
