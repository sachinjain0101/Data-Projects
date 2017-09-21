import org.apache.spark.sql.SparkSession

object FirstTestApp {
  def main(args: Array[String]) {
    val logFile = "C:/Data-Projects/sample-data/out.txt" // Should be some file on your system
    val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("1")).count()
    val numBs = logData.filter(line => line.contains("5")).count()
    println(s"Lines with 1: $numAs, Lines with 5: $numBs")
    spark.stop()
  }
}