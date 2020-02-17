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
    val query ="Rocky_Mountain_National_Park" //"eclipse"

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
      .withColumn("from", $"from".cast(sql.types.LongType))
      .drop("id")
      .distinct()
    baseSet.show(10)
    baseSet.printSchema()
    baseSet.persist()

    //Iterate and calculate Hub/Authority Score
    //-------Iterate here------
    //Calc auth scores
    var auths = baseSet.rdd.flatMap(row =>
      row.getAs[String]("to").split(" ").map(item =>
        (item,row.getAs[Long]("HubScore"))))
      .reduceByKey((a,b) => a + b)
      .toDF("from","AuthScore")
    auths.persist()

    //Normalize the auth scores
    var authsSum = auths.select("AuthScore").rdd.map(row => row.getAs[Long]("AuthScore")).reduce(_+_)
    var normalizedAuths = auths.withColumn("AuthScore", $"AuthScore"/authsSum)
    normalizedAuths.show(10)
    normalizedAuths.persist()

    //Join back into the base set
    baseSet = baseSet.drop("AuthScore").join(normalizedAuths, Seq("from"))
    baseSet.show(10)

    var hubs = baseSet.rdd.flatMap(row =>row.getAs[String]("to").split(" ")
      .map(item => (item, row.getAs[Long]("from"))))
      .toDF("dest", "source")
      .join(normalizedAuths, $"from" === $"dest")
      .drop("from")
      .groupBy($"source").sum("AuthScore")
      .withColumnRenamed("sum(AuthScore)", "HubScore")
      //.withColumn("HubScore",$"HubScore".cast(sql.types.LongType))
    hubs.persist()
    hubs.printSchema()
    hubs.show(10)

//    var hubsSum = hubs.select("HubScore").rdd.map(row => row.getAs[Long]("HubScore")).reduce(_+_)
//    var normalizedHubs = hubs.withColumn("HubScore", $"HubScore"/hubsSum)
//    normalizedHubs.show(10)

    //baseSet.sort($"AuthScore".desc).limit(10).show()

    spark.stop()
  }
}
