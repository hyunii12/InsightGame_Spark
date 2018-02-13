package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object getScore {
  def ltrim(s: String) = s.replaceAll("^\\s+", "");
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    /* 
     * args(0) : news, tgdContents, ruriContents, ranking
     */
    var date = java.time.LocalDate.now.toString;      
    if(args(1) != null || args(1) != "")
      date = args(1);
//    val file = sc.textFile("/data/issuesdata/issues_"+args(0)+"_"+date).map(_.split(", "));
    val news = sc.textFile("/result_spark/news/news_"+date).map(_.split(", "));
    val tgds = sc.textFile("/result_spark/user/tgd_contents_"+date).map(_.split(", "));
    val ruries = sc.textFile("/result_spark/users/ruri_contents_"+date).map(_.split(", "));
    val file = news.union(tgds).union(ruries);
    val fileRdd = file.map(data => new Tuple2(data(0).toLowerCase(), data(1)));
    val file_gameDic = sc.textFile("/result_spark/gameDic").map(_.split("\t")).map{
      data => new Tuple2(data(0), new Tuple2(data(1), data(2)))
    }
    val joinRdd = file_gameDic.join(fileRdd).map(data => (data._1, data._2));
    val result = joinRdd.map(data => new Tuple2(data._2._1._1, data._2._1._2.toDouble*data._2._2.toDouble)	);
    val gameDic = file_gameDic.map(data => new Tuple2(data._2._1, 1.0)).union(result);
    val gameScore = gameDic.reduceByKey((a, b) => a + b).filter(_._2 > 2.0);
    gameScore.map(item => item.swap).sortByKey(false, 1).map(item => item.swap);
    
    val gameFile = sc.textFile("/data/rawdata/gameNames").map(_.split(",")).zipWithIndex().map{case (k,v) => (k(0), v)}
    val issueScore2 = gameFile.map{case (a,b) => (b.toString, a.toString)}.join(gameScore).map{ case(k,v) => (k, v._1, v._2)};
    val issueScore_save = issueScore2.map {case(a,b,c) => Array(a, b, c, date).mkString(", ") };
    issueScore_save.saveAsTextFile("/result_spark/issues/issues_game_score_"+date);
    
  }
}