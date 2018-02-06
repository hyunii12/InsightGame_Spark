package preprocess

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.twitter.penguin.korean.TwitterKoreanProcessor

object newsMeca {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    val file = sc.textFile("/tgd/board_tgd.txt").flatMap(line => line.split('\n'));
    val rdd = file.map(data => data.split(","));
    
    var date = java.time.LocalDate.now.toString;      
    if(args(0) != null || args(0) != "")
      date = args(0);
    val rdd_filtered = rdd.filter(data => data(2) == date);
    val rdd2 = rdd_filtered.map(data => data(0) + data(1));
    val normalized = rdd2.map( data => TwitterKoreanProcessor.normalize(data) )
    val tokens = normalized.flatMap(data => TwitterKoreanProcessor.tokenize(data))
    val tok_filtered = tokens.filter(d => (d.pos).toString contains("Noun"));
    val tgdMap = tok_filtered.map(data => (data.text, 1));
    val tgdMapReduced = tgdMap.reduceByKey(_+_);
    val result = tgdMapReduced.map{ case (k, v) => Array(k, v).mkString(", ")};
    result.saveAsTextFile("/result_spark/tgdContents"+java.time.LocalDate.now.toString);
        
    
  }
}