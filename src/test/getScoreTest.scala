package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object getScoreTest {
case class GAME(gameIdx: Int, title:String, gameWeight: Double, wordsCnt: Int);
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InsightGame_Spark");
    val sc = new SparkContext(conf);
    
    var date = "2018-02-15";
    val news = sc.textFile("/result_spark/news/news_"+date).map(_.split(", "));
    val tgds = sc.textFile("/result_spark/users/tgd_contents_"+date).map(_.split(", "));
    val ruries = sc.textFile("/result_spark/users/ruri_contents_"+date).map(_.split(", "));
    val file = news.union(tgds).union(ruries);
    val fileRdd = file.map(data => new Tuple2(data(0).toLowerCase(), data(1)));
    val file_gameDic = sc.textFile("/result_spark/gameDic").map(_.split("\t")).map( data => (data(1), data(0), data(2)));
//    val gameDF = file_gameDic.map{ case (a0, a1, a2) => GAME(a0.toInt, a1, a2.toDouble, 0) }.toDF()
  }
}