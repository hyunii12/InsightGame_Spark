package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.twitter.penguin.korean.TwitterKoreanProcessor
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object tgdContents {
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    val file = sc.textFile("/tgd/board_tgd.txt").map(line => line.split('\n'));
    val rdd = file.map(data => data.flatMap(attr => attr.split(",")));
    var date = java.time.LocalDate.now.toString;      
    if(args(0) != null || args(0) != "")
      date = args(0);
    val rdd_filtered = rdd.filter(data => data(1) == date);
    rdd_filtered.map(_.mkString(", ")).coalesce(1).saveAsTextFile("/result_spark/tgd/tgd_"+date);
    val rdd2 = rdd_filtered.map(data => data(2) + data(3));
    val normalized = rdd2.map( data => TwitterKoreanProcessor.normalize(data) )
    val tokens = normalized.flatMap(data => TwitterKoreanProcessor.tokenize(data))
    val tok_filtered = tokens.filter(d => {(d.pos).toString contains "Noun"} 
//      || {(d.pos).toString contains "Number"}
      || {(d.pos).toString contains "Alpha"});
    val tgdWords = tok_filtered.map(data => (data.text, 1.toDouble));
    val tgdWordsReduced = tgdWords.reduceByKey(_+_);
    val result = tgdWordsReduced.map{ case (k, v) => Array(k, v).mkString(", ")};
    result.saveAsTextFile("/result_spark/users/tgd_contents_"+date);
  }
}