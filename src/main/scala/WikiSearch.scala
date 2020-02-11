import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.SparkContext._

object WikiSearch {
  //Spark Session
  val spark = SparkSession.builder.appName("WikiSearch").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]) {
    //File locations
    val titlesFile = "hdfs://richmond:32251/user/millerr/titles-sorted.txt"
    val linksFile = "hdfs://richmond:32251/user/millerr/links-simple-sorted.txt"

    //Search string
    val query = "Rocky_Mountain_National_Park"

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
      .withColumn("AuthScore", lit(1))
      .withColumn("HubScore", lit(1))
      .withColumnRenamed("id","root_set_id")
    baseSet.show(10)
    baseSet.persist()


    //Iterate and calculate Hub/Authority Score
    val hubs = baseSet.map(row => (if(row.get(0) != row.get(1)) (row.get(0),row.get(3)) else (row.get(0),0))).reduceByKey(_ + _)
    hubs.show(10) 


    spark.stop()
  }
}
