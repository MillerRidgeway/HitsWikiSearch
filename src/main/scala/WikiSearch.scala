import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql

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
    rootSet.persist()

    //Base set generation
    var baseSet = rootSet.join(linksDf, $"id" === $"from" || linksDf("to").contains($"id"))
      .withColumn("AuthScore", lit(1L))
      .withColumn("HubScore", lit(1L))
      .withColumnRenamed("id","root_set_id")
      .withColumn("from", $"from".cast(sql.types.LongType))
    baseSet.show(10)
    baseSet.printSchema()
    baseSet.persist()

    //Iterate and calculate Hub/Authority Score

    //Calculate hubs score in new DF, normalize, then join back with base
    val auths = baseSet.rdd.map(row => if(row.getAs[Long](0) != row.getAs[Long](1))
      (row.getAs[Long](1),row.getAs[Long](3)) else (row.getAs[Long](0),0L))
      .reduceByKey((a,b) => a + b)
      .toDF("from","AuthScore")
    auths.show(10)
    auths.persist()

    val authsSum = auths.select("AuthScore").rdd.map(row => row.getAs[Long]("AuthScore")).reduce(_+_)
    val normalizedAuths = auths.withColumn("AuthScore", $"AuthScore"/authsSum)

    baseSet = baseSet.join(normalizedAuths, Seq("from"), "left")
    baseSet.show(10)

    baseSet.sort($"AuthScore".desc).limit(10).show()



    spark.stop()
  }

}
