package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object getScore {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    // args(0) : news, tgdContents, ruriContents, ranking
    
//    val file = sc.textFile("/data/issuesdata/issues_"+args(0));
    val file = sc.textFile("/data/issuesdata/issues_news2018-02-09");
    
    
  }
}