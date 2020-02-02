import org.apache.spark.sql.SparkSession


case class Title(id: Int, name: String)
object WikiSearch {
  def main(args: Array[String]) {
    //Files
    val titlesFile = "hdfs://richmond:32251/user/millerr/titles-sorted.txt"
    val linksFile = "hdfs://richmond:32251/user/millerr/links-simple-sorted.txt"

    //Spark Session
    val spark = SparkSession.builder.appName("WikiSearch").getOrCreate()

    val titlesDf = spark.read.textFile(titlesFile).rdd.zipWithIndex()
    val linksDf = spark.read.textFile(linksFile)

    println(titlesDf.first())
    println(linksDf.first())
    
    spark.stop()
  }
}
