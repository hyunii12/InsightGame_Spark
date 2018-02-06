package preprocess

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.phrase_extractor.KoreanPhraseExtractor.KoreanPhrase
import com.twitter.penguin.korean.tokenizer.KoreanTokenizer.KoreanToken

object tgdContents {
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    val file = sc.textFile("/tgd/board_tgd.txt").map(line => line.split('\n'));
    val rdd = file.map(data => data.flatMap(attr => attr.split(",")));
    var date = java.time.LocalDate.now.toString;      
    if(args(0) != null || args(0) != "")
      date = args(0);
    val rdd_filtered = rdd.filter(data => data(1) == date);
    val rdd2 = rdd_filtered.map(data => data(2) + data(3));
    
    val normalized = rdd2.map( data => TwitterKoreanProcessor.normalize(data) )
    val tokens = normalized.flatMap(data => TwitterKoreanProcessor.tokenize(data))
    val tok_filtered = tokens.filter(d => (d.pos).toString contains("Noun"));
    val tgdMap = tok_filtered.map(data => (data.text, 1.toDouble));
    val tgdMapReduced = tgdMap.reduceByKey(_+_);
    val result = tgdMapReduced.map{ case (k, v) => Array(k, v).mkString(", ")};
    result.saveAsTextFile("/result_spark/tgdContents"+date);
    
    // 이제 리듀서 만들자..........
    val game1 = sc.textFile("/data/predata/pre_gamenames.txt");
    val game2 = sc.textFile("/data/predata/game_weights.txt");
    val game = game1.union(game2);
    val gameRdd = game.map(data => data.split("\n"));
    val gameRdd2 = gameRdd.map(data => data(0).split(","));
    val gameMap = gameRdd2.map(data => (data(0), data(1).toDouble));
    
    
  }
}