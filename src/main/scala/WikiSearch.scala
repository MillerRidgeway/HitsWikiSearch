import org.apache.spark.sql.SparkSession
object WikiSearch {
  def main(args: Array[String]) {
    //Files
    val titlesFile = "hdfs://richmond:32251/user/millerr/titles-sorted.txt"
    val linksFile = "hdfs://richmond:32251/user/millerr/links-simple-sorted.txt"

    //Spark Session
    val spark = SparkSession.builder.appName("WikiSearch").getOrCreate()

    //Read in files as dataframes
    val titlesDf = spark.read.textFile(titlesFile).rdd.zipWithIndex()
    val linksDf = spark.read.textFile(linksFile)

    //titlesDf.take(10).foreach(println)
    //linksDf.take(10).foreach(println)

    //Root set generation
    val query = "Colorado_State_University"
    val rootSet = titlesDf.filter(s => s._1.contains(query))

    rootSet.take(10).foreach(println)

    spark.stop()
  }
}
