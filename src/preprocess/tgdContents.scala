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
    val tgdWords = tok_filtered.map(data => (data.text, 1.toDouble));
    val tgdWordsReduced = tgdWords.reduceByKey(_+_);
    val result = tgdWordsReduced.map{ case (k, v) => Array(k, v).mkString(", ")};
    result.saveAsTextFile("/result_spark/tgdContents"+date);
    
    // 이제 리듀서 만들자..........
    val game1 = sc.textFile("/data/predata/pre_gamenames.txt");
    val game2 = sc.textFile("/data/predata/game_weights.txt");
    val gameresult = game1.union(game2);
    val gameresult2 = gameresult.map(data => data.split("\n"))
    val gameMap = gameresult2.map(data => data(0).split(","))
    val gameDic = gameMap.map(data => (data(0), data(1).toDouble));
    
    val bcast = sc.broadcast(gameDic.map(_._1).collect());
    val tgdFilteredBybcast = tgdWordsReduced.filter(r => bcast.value.contains(r._1)); 
    val resultRdd = tgdFilteredBybcast.join(gameDic);
    val reducedRdd = resultRdd.map{case (k,v) => (k, v._1*v._2)}
    val reducedFilteredRdd = reducedRdd.filter{ case (k,v) => v != 1.0 }
    val reducedResult = reducedFilteredRdd.map{ case (k,v) => Array(k, v).mkString(", ")};
    reducedResult.saveAsTextFile("/result_spark/issues_news"+date);
  }
}