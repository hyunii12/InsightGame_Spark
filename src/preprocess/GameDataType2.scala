package preprocess

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GameDataType2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"))
    val file = sc.textFile("/data/rawdata/gameNames")
    val gameRdd = file.map(_.split(",")).zipWithIndex();
    val gameMap = gameRdd.map(data => data._1.map(attr => (ltrim(attr), data._2, 1.0))).flatMap(attr => attr);
    gameMap.saveAsTextFile("/result_spark/gameNames");
    val file2 = sc.textFile("/data/rawdata/gameNames_weight").map(_.split("\t"));
    val gameRdd2 = file2.map(data => (data(0).split(","), data(1), data(2)));
    val gameMap2 = gameRdd2.map(data => data._1.map(attr => (ltrim(attr), data._3.toLong, data._2.toDouble))).flatMap(attr => attr);
    val gameSet = gameMap.union(gameMap2);
//    gameSet.sortBy(_._2, true).foreach(println(_))
    gameSet.saveAsTextFile("/result_spark/gameDic")
  }
  
  def ltrim(s: String) = s.replaceAll("^\\s+", "")
  def rtrim(s: String) = s.replaceAll("\\s+$", "")
}