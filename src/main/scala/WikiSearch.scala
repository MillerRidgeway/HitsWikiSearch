import org.apache.spark.sql.SparkSession

object WikiSearch {
  //Spark Session
  val spark = SparkSession.builder.appName("WikiSearch").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]) {
    //File locations
    val titlesFile = "hdfs://richmond:32251/user/millerr/titles-sorted.txt"
    val linksFile = "hdfs://richmond:32251/user/millerr/links-simple-sorted.txt"

    //Search string
    val query = "Colorado_State_University"

    //Read in files - links as dataframe, titles as RDD (later converted)
    val titlesRdd = spark.read.textFile(titlesFile).rdd.zipWithIndex()
    val linksDf = spark.read.option("delimiter", ":").csv(linksFile).toDF("from", "to")
    titlesRdd.take(10).foreach(println)
    linksDf.show(10)

    //Root set generation (dataframe)
    val rootSet = titlesRdd.filter(s => s._1.contains(query)).map(x => x._2).toDF("id")
    rootSet.show(10)
    //println(rootSet.count())

    //Base set generation
    val queryLinks = rootSet.join(linksDf, $"id" === $"from" || linksDf("to").contains($"id")).drop("id")
    queryLinks.show(10)
    //println(queryLinks.count())

    //val toColStrings = queryLinks.select("to").collect.map(row => row.getString(0))
    //toColStrings.map(line => line.split(" ").collect
    spark.stop()
  }
}
