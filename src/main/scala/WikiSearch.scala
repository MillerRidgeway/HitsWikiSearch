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
      .drop("id")
      .withColumn("from", $"from".cast(sql.types.LongType))
      .distinct()
    baseSet.show(10)
    baseSet.printSchema()
    baseSet.persist()

    //Iterate and calculate Hub/Authority Score

    //Calc auth scores
    val auths = baseSet.rdd.flatMap(row =>
      row.getAs[String]("to").split(" ").map(item =>
        (item,row.getAs[Long]("HubScore"))))
      .reduceByKey((a,b) => a + b)
      .toDF("from","AuthScore")
    auths.persist()

    //Normalize the auth scores
    val authsSum = auths.select("AuthScore").rdd.map(row => row.getAs[Long]("AuthScore")).reduce(_+_)
    val normalizedAuths = auths.withColumn("AuthScore", $"AuthScore"/authsSum)
    normalizedAuths.show(10)
    normalizedAuths.persist()

    //Join back into the base set
    baseSet = baseSet.drop("AuthScore").join(normalizedAuths, Seq("from"))
    baseSet.show(10)

    val hubs = baseSet.rdd.flatMap(row =>
      row.getAs[String]("to").split(" ").map(item =>
        (row.getAs[Long]("from"),
          if(baseSet.filter($"from" === item).head(1).isEmpty) 0
          else baseSet.filter($"from" === item).first().getAs[Long]("AuthScore"))))
      .reduceByKey((a,b) => a + b)
      .toDF("from", "HubScore")
    hubs.persist()
    hubs.show(10)

    //baseSet.sort($"AuthScore".desc).limit(10).show()

    spark.stop()
  }
}
