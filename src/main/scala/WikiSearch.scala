import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


case class TitleRecord(name: String, id: Long)

object WikiSearch {
  def main(args: Array[String]) {
    //File locations
    val titlesFile = "hdfs://richmond:32251/user/millerr/titles-sorted.txt"
    val linksFile = "hdfs://richmond:32251/user/millerr/links-simple-sorted.txt"

    //Search string
    val query = "Colorado_State_University"

    //Spark Session
    val spark = SparkSession.builder.appName("WikiSearch").getOrCreate()

    //Read in files - links as dataframe, titles as RDD
    val titlesRdd = spark.read.textFile(titlesFile).rdd.zipWithIndex()
    val linksDf = spark.read.option("delimiter", ":").csv(linksFile).toDF("from", "to")
    titlesRdd.take(10).foreach(println)
    linksDf.take(10).foreach(println)

    //Root set generation
    val rootSet = titlesRdd.filter(s => s._1.contains(query)).map(x => x._2)
    println(rootSet.count())
    rootSet.take(10).foreach(println)

    //Base set generation
    //val baseSet =



    spark.stop()
  }
}
