import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object WikiSearch{
  def main(args: Array[String]) {
    //Spark Session
    val sc = SparkSession.builder.appName("WikiSearch").getOrCreate().sparkContext

    //Search string
    val query = args(0)

    //Files
    val titlesFile = sc.textFile("hdfs://richmond:32251/user/millerr/titles-sorted.txt")
    val linksFile = sc.textFile("hdfs://richmond:32251/user/millerr/links-simple-sorted.txt")

    //Read in titles, zip with line number and filter by query
    val titles = titlesFile.zipWithIndex().map{case(k, v) => (v + 1, k)}
    val rootSet = titles.filter{case(k, v) => v.contains(query)}

    //Build graph set of all (from, to)
    val graphSet = linksFile
      .filter(s => !s.endsWith(": "))
      .map(s => (s.split(':')(0).toLong, s.split(':')(1).trim.split(' ')))
      .flatMapValues(x => x)
      .map{case(k,v) => (k, v.toLong)}
    graphSet.take(30).foreach(println)

    //Flip (from, to) to (to, from) and filter by root
    val destToSource = graphSet
      .map{case(k,v) => (v,k)}
      .join(rootSet)
      .map{case(k,(v1,v2)) => (v1,k)}
    destToSource.take(30).foreach(println)

    //Keep (from,to) and filter by root
    val sourceToDest = graphSet
      .join(rootSet)
      .map{case(k,(v1,v2)) => (k,v1)}
    sourceToDest.take(30).foreach(println)

    //Combine to form 'base set' without titles
    var iterSet = sourceToDest
      .union(destToSource)
      .distinct()
      .sortByKey()
      .persist()
    iterSet.take(30).foreach(println)

    //Base set with titles, used for later join
    var baseSet = sourceToDest
      .map{case(k,v) => (v,k)}
      .union(destToSource)
      .groupByKey()
      .join(titles)
      .map{case(k,(v1,v2)) => (k,v2)}
      .union(rootSet)
      .distinct()
      .sortByKey()
      .persist()
    baseSet.take(30).foreach(println)

    //Hubs/Auths
    var authsSet = baseSet.map{case(k,v) => (k, 1D)}
    var hubsSet = baseSet.map{case(k,v) => (k, 1D)}

    for(i <- 1 to 50){
      //Calc auth score from hub score discarding nulls (emit 0)
      authsSet = iterSet.join(hubsSet)
        .map{case(k,(v1,v2)) => (v1,v2)}
        .reduceByKey((x, y) => x+y)
        .rightOuterJoin(hubsSet)
        .map{case(k,(Some(v1),v2)) => (k, v1);
             case(k,(None,v2)) => (k,0)}

      //Normalize auths
      var normalizeAuths = authsSet.map(p => p._2).sum()
      authsSet = authsSet
        .map{case(k,v) => (k, v/normalizeAuths)}
        .persist()

      //Flip keys to calculate hub from auth score discarding nulls (emit 0)
      hubsSet = iterSet
        .map{case(k,v) => (v,k)}
        .join(authsSet)
        .map{case(k,(v1,v2)) => (v1,v2)}
        .reduceByKey((x, y) => x+y)
        .rightOuterJoin(authsSet)
        .map{case(k,(Some(v1),v2)) => (k, v1);
             case(k,(None,v2)) => (k,0)}

      //Normalize hubs
      var normalizeHubs = hubsSet.map(p => p._2).sum()
      hubsSet = hubsSet
        .map{case(k,v) => (k, v/normalizeHubs)}
        .persist()
    }


    var authsFinal = authsSet.join(baseSet).map{case(k,(v1,v2)) => (v1,v2)}.sortByKey(false)
    authsFinal.saveAsTextFile("hdfs://richmond:32251/user/millerr/testing_eclipse_auths_7.txt")
    var hubsFinal = hubsSet.join(baseSet).map{case(k,(v1,v2)) => (v1,v2)}.sortByKey(false)
    hubsFinal.saveAsTextFile("hdfs://richmond:32251/user/millerr/testing_eclipse_hubs_7.txt")

  }
}