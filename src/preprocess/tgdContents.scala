package preprocess

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.openkoreantext.processor.tokenizer.KoreanTokenizer.KoreanToken
import org.openkoreantext.processor.OpenKoreanTextProcessor

object tgdContents {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    val file = sc.textFile("/tgd/board_tgd.txt").map(line => line.split('\n'));

    val rdd = file.map(data => data.flatMap(attr => attr.split(",")));

    val rdd_filtered = rdd.filter(data => data(1) == java.time.LocalDate.now.toString);
    val rdd2 = rdd_filtered.map(data => data(2) + data(3));

    val normalized: CharSequence = OpenKoreanTextProcessor.normalize(rdd2.toString());
    val tokens: Seq[KoreanToken] = OpenKoreanTextProcessor.tokenize(normalized);
    println(tokens);
    
    val tokensRdd = sc.parallelize(tokens);
    val result = tokensRdd.map(data => (data, 1));
    
    result.collect.take(10);   
    
    result.coalesce(1).saveAsTextFile("/result_spark/tgd2222");

  }
}