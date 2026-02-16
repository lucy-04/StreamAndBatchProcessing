import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BatchProcessor")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("sales_data.csv")

    println("--- Batch Processing Results ---")

    val report = df.groupBy("product").sum("amount")

    report.show()
    
    spark.stop()
  }
}
