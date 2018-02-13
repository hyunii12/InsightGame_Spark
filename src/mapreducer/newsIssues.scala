package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.twitter.penguin.korean.TwitterKoreanProcessor
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object newsIssues {
  def main(args: Array[String]): Unit = {
    // 뉴스기사 매퍼 생성
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    val file_news_meca = sc.textFile("/meca/news_meca.txt").flatMap(_.split('\n'));
    val file_news_inven = sc.textFile("/inven/news_inven.txt").flatMap(_.split("\n"));
    val mecaRdd = file_news_meca.map(_.split(","));
    val invenRdd = file_news_inven.map(_.split("§§"));
    val newsRdd = mecaRdd.union(invenRdd);
    val newsRdd2 = newsRdd.filter(data => data.length == 3);
    var date = java.time.LocalDate.now.toString;      
    if(args(0) != null || args(0) != "")
      date = args(0);
    
    val rdd_filtered = newsRdd2.filter(data => data(2) == date);
    val rdd = rdd_filtered.map(data => data(0) + data(1));
//    import com.twitter.penguin.korean.TwitterKoreanProcessor
    val normalized = rdd.map( data => TwitterKoreanProcessor.normalize(data) )
    val tokens = normalized.flatMap(data => TwitterKoreanProcessor.tokenize(data))
    val tok_filtered = tokens.filter(d => {(d.pos).toString contains "Noun"} 
      || {(d.pos).toString contains "Number"}
      || {(d.pos).toString contains "Alpha"});
    val newsWords = tok_filtered.map(data => (data.text, 1.0)); 
    val newsWordsReduced = newsWords.reduceByKey(_+_).filter(data => data._2 > 1.0);
    val newsSortedByValue = newsWordsReduced.map(item => item.swap).sortByKey(false, 1).map(item => item.swap);
    val newsWordsResult = newsSortedByValue.map{ case (k, v) => Array(k, v).mkString(", ")};
    // 매퍼 저장
    newsWordsResult.saveAsTextFile("/result_spark/news/news_"+date);

  }
}