import org.apache.spark.sql.SparkSession

case class TitleRecord(name: String, id: Long)

object WikiSearch {
  def main(args: Array[String]) {
    //Files
    val titlesFile = "hdfs://richmond:32251/user/millerr/titles-sorted.txt"
    val linksFile = "hdfs://richmond:32251/user/millerr/links-simple-sorted.txt"

    //Query
    val query = "Colorado_State_University"

    //Spark Session
    val spark = SparkSession.builder.appName("WikiSearch").getOrCreate()

    //Read in files as dataframes
    val titlesRdd = spark.read.textFile(titlesFile).rdd.zipWithIndex()
    val linksDf = spark.read.option("delimiter", ":").csv(linksFile).toDF("from", "to")



    //titlesDf.take(10).foreach(println)
    //linksDf.take(10).foreach(println)

    //Root set generation
    val rootSet = titlesRdd.filter(s => s._1.contains(query))

    //Base set generation
    val baseSet =

    rootSet.take(10).foreach(println)

    spark.stop()
  }
}
