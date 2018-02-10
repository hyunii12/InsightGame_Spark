package preprocess

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GameDic {
  def ltrim(s: String) = s.replaceAll("^\\s+", "");
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InsightGameSpark");
    val sc = new SparkContext(conf);
    val file = sc.textFile("file:///home/hadoop/data/rawdata3/gameDic");
    val games = file.flatMap(_.split("\n")).map(_.split(",")).map(new Tuple2(_, 1.0));
    val games_toString = games.map{case (k,v) => Array(k,v).mkString(",")};
    games_toString.coalesce(1).saveAsTextFile("/result_spark/gameDic");
  }
}