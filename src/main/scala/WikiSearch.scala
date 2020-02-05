import org.apache.spark.sql.SparkSession

object WikiSearch {
  //Spark Session
  val spark = SparkSession.builder.appName("WikiSearch").getOrCreate()
  import spark.implicits._
  import spark.sparkContext._

  def main(args: Array[String]) {
    //File locations
    val titlesFile = "hdfs://richmond:32251/user/millerr/titles-sorted.txt"
    val linksFile = "hdfs://richmond:32251/user/millerr/links-simple-sorted.txt"

    //Search string
    val query = "eclipse"

    //Read in files - links as dataframe, titles as RDD (later converted)
    val titlesRdd = spark.read.textFile(titlesFile).rdd.zipWithIndex()
    val linksDf = spark.read.option("delimiter", ":").csv(linksFile).toDF("from", "to")
    titlesRdd.take(10).foreach(println)
    linksDf.show(10)

    //Root set generation (dataframe)
    val rootSet = titlesRdd.filter(s => s._1.contains(query)).map(x => x._2).toDF("id")
    rootSet.show(10)
    println(rootSet.count())

    //Base set generation
    val queryLinks = rootSet.join(linksDf, $"id" === $"from")
    queryLinks.show(10)

    val baseSet = queryLinks.select("to")
    baseSet.show(10)


    spark.stop()
  }
}
