package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object games {
  def ltrim(s: String) = s.replaceAll("^\\s+", "")
  def rtrim(s: String) = s.replaceAll("\\s+$", "")
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    val game1 = sc.textFile("/data/predata/pre_gamenames.txt");
    val game2 = sc.textFile("/data/predata/game_weights.txt");
    val result = game1.union(game2)
    val result2 = result.map(data => data.split("\n"))
    val gameMap = result2.map(data => data(0).split(","))
    val gameDic = gameMap.map(data => (data(0), data(1).toDouble))
    // 이제 리듀서 만들기
  }
}