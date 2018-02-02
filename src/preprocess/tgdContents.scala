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

//    val rdd_filtered = rdd.filter(data => data(1) == java.time.LocalDate.now.toString);
    val rdd_filtered = rdd.filter(data => data(1) == "2018-01-29");
     
    val rdd2 = rdd_filtered.map(data => data(2) + data(3));
    val normalized: CharSequence = TwitterKoreanProcessor.normalize(rdd2.collect());
    val tokens: Seq[KoreanToken] = TwitterKoreanProcessor.tokenize(normalized)
    
    val tokensRdd = sc.parallelize(tokens);
    val result = tokensRdd.map(data => (data, 1));
    
    result.collect.take(10);
    
    result.coalesce(1).saveAsTextFile("/result_spark/tgd2222");

  }
}