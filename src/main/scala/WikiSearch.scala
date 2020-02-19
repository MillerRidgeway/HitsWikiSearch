import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit,udf}
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
    val query = args(0) //"Rocky_Mountain_National_Park"

    //Read in files - links as dataframe, titles as RDD and DF
    val titlesRdd = spark.read.textFile(titlesFile).rdd.zipWithIndex()
    val titlesIndex = titlesRdd.toDF("name","id")
    val linksDf = spark.read.option("delimiter", ":").csv(linksFile).toDF("from", "to")

    //Root set generation (dataframe)
    val rootSet = titlesRdd.filter(s => s._1.contains(query)).map(x => x._2).toDF("id")

    //Base set generation
    var baseSet = rootSet.join(linksDf, $"id" === $"from" || linksDf("to").contains($"id"))
      .withColumn("AuthScore", lit(1D))
      .withColumn("HubScore", lit(1D))
      .withColumn("from", $"from".cast(sql.types.DoubleType))
      .drop("id")
      .distinct()
    baseSet.persist()

    //relationship for the 3 pair
    val hubsSet = baseSet.rdd.flatMap(row => row.getAs[String]("to").split(" ")
      .map(item => (item, row.getAs[Double]("from"))))
      .toDF("dest", "source")

    //Iterate and calculate Hub/Authority Score
    for(i <- 1 to 10) {
      //Calc auth scores using 'to' column
      var auths = baseSet.rdd.flatMap(row =>
        row.getAs[String]("to").split(" ").map(item =>
          (item, row.getAs[Double]("HubScore"))))
        .reduceByKey((a, b) => a + b)
        .toDF("from", "AuthScore")

      //Normalize the auth scores
      var authsSum = auths.select("AuthScore").rdd.map(row => row.getAs[Double]("AuthScore")).reduce(_ + _)
      var normalizedAuths = auths.withColumn("AuthScore", $"AuthScore" / authsSum)
      normalizedAuths.persist()

      //Join back into the base set
      baseSet = baseSet.drop("AuthScore").join(normalizedAuths, Seq("from"))

      //Calc hub scores via 3 tuple 'to' manipulation
      var hubs = hubsSet.join(normalizedAuths, $"from" === $"dest")
        .drop("from")
        .drop("dest")
        .rdd
        .map(row => (row.getAs[Double](0), row.getAs[Double](0)))
        .reduceByKey((a, b) => a + b)
        .toDF("source", "HubScore")

      //Normalize the hub scores
      var hubsSum = hubs.select("HubScore").rdd.map(row => row.getAs[Double]("HubScore")).reduce(_ + _)
      var normalizedHubs = hubs.withColumn("HubScore", $"HubScore" / hubsSum)
      normalizedHubs.persist()

      //Join back into base set
      baseSet = baseSet.drop("HubScore")
        .join(normalizedHubs, $"from" === $"source")
        .drop("source")
    }


    val authsSortedDf = baseSet.join(titlesIndex, $"from" === $"id")
      .drop("to")
      .drop("id")
      .sort($"AuthScore".desc)
      .limit(50)

    val authsSorted = authsSortedDf.rdd

    val hubsSorted = authsSortedDf
      .sort($"HubScore".desc)
      .limit(50)
      .rdd

    authsSorted.saveAsTextFile("hdfs://richmond:32251/user/millerr/testing_eclipse_auths_3.txt")
    hubsSorted.saveAsTextFile("hdfs://richmond:32251/user/millerr/testing_eclipse_hubs_3.txt")

    spark.stop()
  }
}
