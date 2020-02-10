import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

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

    //Base set generation
    val baseSet = rootSet.join(linksDf, $"id" === $"from" || linksDf("to").contains($"id"))
      .drop("id")
      .withColumn("AuthScore", lit(1))
      .withColumn("HubScore", lit(1))
    baseSet.show(10)
    baseSet.persist()


    //Iterate and calculate Hub/Authority Score
    baseSet.map(row => row.getAs[String](2).split(" ")
      .map(id => baseSet.filter(baseSet("from") === id)("HubScore")) )


    spark.stop()
  }
}
