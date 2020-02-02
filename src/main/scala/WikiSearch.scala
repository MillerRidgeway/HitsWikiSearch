import org.apache.spark.sql.SparkSession

object WikiSearch {
  def main(args: Array[String]) {
    val titlesFile = "hdfs://richmond:32251/user/millerr/titles-sorted.txt"
    val linksFile = "hdfs://richmond:32251/user/millerr/links-simple-sorted.txt"
    val spark = SparkSession.builder.appName("WikiSearch").getOrCreate()

    println("Hello, World!")
    spark.stop()
  }
}
