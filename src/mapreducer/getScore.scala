package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object getScore {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("InsightGame_Spark");
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder().appName("InsightGame_Spark").master("local[*]").getOrCreate()
//    var date = "2018-02-18";      
    var date = java.time.LocalDate.now.toString;      
    if(args(0) != null || args(0) != "")
      date = args(0);
    val news = sc.textFile("/result_spark/news/news_"+date).map(_.split(", "));
    val tgds = sc.textFile("/result_spark/users/tgd_contents_"+date).map(_.split(", "));
    val ruries = sc.textFile("/result_spark/users/ruri_contents_"+date).map(_.split(", "));
    val file = news.union(tgds).union(ruries);
    val fileRdd = file.map(data => new Tuple2(data(0).toLowerCase(), data(1).toDouble));
    val fileRdd2 = fileRdd.reduceByKey(_+_);
    val file_gameDic = sc.textFile("/result_spark/gameDic").map(_.split("\t")).map( data => (data(1), data(0), data(2)));
    val file_ranking = sc.textFile("/data/predata/ranking.txt").map(_.split(", ")).map(data => new Tuple2(data(0), data(1)))
    import spark.implicits._
    val fileDF = spark.createDataFrame(fileRdd2).toDF("title":String, "wordsCnt":String);
    val gameDF = spark.createDataFrame(file_gameDic).toDF("gameIdx", "title", "gameWeight");
    val rankDF = spark.createDataFrame(file_ranking).toDF("title", "rank");
    val gameDF_file = gameDF.join(fileDF, "title").select($"gameIdx", $"title", $"gameWeight", $"wordsCnt" * $"gameWeight").toDF("gameIdx", "title", "weight", "wc_score");
    val gameDF_rank = gameDF.join(rankDF, "title");
    
    // gameDic <-> contents
    gameDF_file.createOrReplaceTempView("tmp_file");
    val wcWordingDF = spark.sql("select a.gameIdx, first(a.title) as title, a.weight, min(a.wc_score) as wc_score from tmp_file a, tmp_file b where a.gameIdx = b.gameIdx and a.weight < 1.0 group by a.gameIdx, a.weight having count(*) > 1");
    val wcGameDF = spark.sql("select * from tmp_file where weight = 1.0")
    val wcJoinDF = wcWordingDF.union(wcGameDF);
    wcJoinDF.createOrReplaceTempView("tmp_wc_join");
    val resultRdd_file = spark.sql("select gameIdx, sum(wc_score) as score from tmp_wc_join group by gameIdx").select("gameIdx","score").rdd.map{data => (data(0).toString, data(1).toString)};
    
    // gameDic <-> ranking
    gameDF_rank.createOrReplaceTempView("tmp_rank");
    val rkGameDF = spark.sql("select gameIdx, title, 100/mean(rank) as rank from tmp_rank group by gameIdx, title");
    val resultRdd_rank = rkGameDF.select("gameIdx", "rank").rdd.map{data => (data(0).toString, data(1).toString)};
    
    val gameLabel = sc.textFile("/data/predata/gameLabel.txt").map(_.split("\t")).map(data => (data(1), data(0)));
    val resultRdd_union = resultRdd_file.union(resultRdd_rank);
    val resultRdd = gameLabel.leftOuterJoin(resultRdd_union).filter{ case( a, (b, c)) => c.nonEmpty};
    val resultRdd2 = resultRdd.map{ case (a,(b,c)) => ((a, b), c.getOrElse("not given").toDouble)}
    val resultRdd2_reduced = resultRdd2.reduceByKey(_+_).map{ case ((a, b), c) => Array(a, b, c, date).mkString("\t")};
    resultRdd2_reduced.saveAsTextFile("/result_spark/issues/issues_game_score_"+date)
//    val resultRdd2_reduced = resultRdd2.reduceByKey(_+_).map(item => item.swap).sortByKey(false, 1).map(item=>item.swap).map{ case ((a, b), c) => (a, b, c)};
    
//    val rdd = gameLabel.leftOuterJoin(resultRdd_file).filter{ case(a,(b,c)) => c.nonEmpty}.map{ case (a,(b,c)) => ((a, b), c.getOrElse("not given").toDouble)}.reduceByKey(_+_).map{ case ((a, b), c) => (a, b, c)}
//    val rdd2 = gameLabel.leftOuterJoin(resultRdd_rank).filter{ case(a,(b,c)) => c.nonEmpty}.map{ case (a,(b,c)) => ((a, b), c.getOrElse("(not given)").toDouble)}.reduceByKey(_+_).map{ case ((a, b), c) => (a, b, c)}
    
    
    
    
  }
}