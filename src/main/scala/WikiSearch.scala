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
    val query = "Rocky_Mountain_National_Park"//"eclipse"

    //Read in files - links as dataframe, titles as RDD and DF
    val titlesRdd = spark.read.textFile(titlesFile).rdd.zipWithIndex()
    val titlesIndex = titlesRdd.toDF("name","id")
    val linksDf = spark.read.option("delimiter", ":").csv(linksFile).toDF("from", "to")
    titlesRdd.take(10).foreach(println)
    titlesIndex.show(10)
    linksDf.show(10)

    //Root set generation (dataframe)
    val rootSet = titlesRdd.filter(s => s._1.contains(query)).map(x => x._2).toDF("id")
    rootSet.show(10)
    //rootSet.persist()

    //Base set generation
    var baseSet = rootSet.join(linksDf, $"id" === $"from" || linksDf("to").contains($"id"))
      .withColumn("AuthScore", lit(1D))
      .withColumn("HubScore", lit(1D))
      .withColumn("from", $"from".cast(sql.types.DoubleType))
      .drop("id")
      .distinct()
    baseSet.show(10)
    baseSet.printSchema()
    baseSet.persist()

    var hubsSet = baseSet.rdd.flatMap(row => row.getAs[String]("to").split(" ")
      .map(item => (item, row.getAs[Double]("from"))))
      .toDF("dest", "source")
      .filter("dest != ''")
      .withColumn("dest", $"dest".cast(sql.types.DoubleType))
    hubsSet.show(10)
    hubsSet.printSchema()
    hubsSet.persist()

    var authsSet = baseSet.rdd.flatMap(row =>
      row.getAs[String]("to").split(" ").map(item =>
        (item, row.getAs[Double]("HubScore"))))
      .toDF("from", "HubScore")
      .filter("from != ''")
      .withColumn("from", $"from".cast(sql.types.DoubleType))
    authsSet.show(10)
    authsSet.printSchema()
    authsSet.persist()

    baseSet = baseSet.drop("HubScore").drop("AuthScore")
    //Iterate and calculate Hub/Authority Score
    for(i <- 1 to 1 ) {
      //Calc auth scores using 'to' column
      var auths = authsSet
        .rdd
        .map(row => (row.getAs[Double](0),row.getAs[Double](1)))
        .reduceByKey((a,b) => a + b)
        .toDF("from", "AuthScore")
      auths.persist()

      //Normalize the auth scores
      var authsSum = auths.select("AuthScore").rdd.map(row => row.getAs[Double]("AuthScore")).reduce(_ + _)
      var normalizedAuths = auths.withColumn("AuthScore", $"AuthScore" / authsSum)
      normalizedAuths.persist()

      //Join back into the base set
      authsSet = authsSet.drop("AuthScore").join(normalizedAuths, Seq("from"))

      //Calc hub scores via 3 tuple 'to' manipulation
      var hubs = hubsSet
        .join(normalizedAuths, $"from" === $"dest")
        .drop("from")
        .rdd
        .map(row => (row.getAs[Double](0), row.getAs[Double](1)))
        .reduceByKey((a,b) => a + b)
        .toDF("source", "HubScore")

      hubs.persist()
      hubs.printSchema()
      hubs.show(10)

      //Normalize the hub scores
      var hubsSum = hubs.select("HubScore").rdd.map(row => row.getAs[Double]("HubScore")).reduce(_ + _)
      var normalizedHubs = hubs.withColumn("HubScore", $"HubScore" / hubsSum)
      //normalizedHubs.show(10)
      normalizedHubs.persist()

      //Join back into base set
      hubsSet = hubsSet.drop("HubScore").join(normalizedHubs, Seq("source"))
      //baseSet.show(10)
    }
    hubsSet = hubsSet
      .withColumnRenamed("source", "from")
      .withColumnRenamed("HubScore", "HubScoreFinal")
    
    baseSet = baseSet.join(hubsSet, Seq("from"))
    baseSet = baseSet.join(authsSet, Seq("from"))

    val authsSorted = baseSet.join(titlesIndex, $"from" === $"id")
      .drop("to")
      .drop("id")
      .drop("dest")
      .drop("HubScore")
      .sort($"AuthScore".desc)
      .limit(50)
      .rdd

    val hubsSorted = baseSet.join(titlesIndex, $"from" === $"id")
      .drop("to")
      .drop("id")
      .drop("dest")
      .drop("HubScore")
      .sort($"HubScoreFinal".desc)
      .limit(50)
      .rdd

    authsSorted.saveAsTextFile("hdfs://richmond:32251/user/millerr/rmnp_2_auth.txt")
    hubsSorted.saveAsTextFile("hdfs://richmond:32251/user/millerr/rmnp_2_hub.txt")

    spark.stop()
  }
}
