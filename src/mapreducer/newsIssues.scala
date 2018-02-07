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
    
    var date = java.time.LocalDate.now.toString;      
    if(args(0) != null || args(0) != "")
      date = args(0);
    
    val rdd_filtered = newsRdd.filter(data => data(2) == date);
    val rdd = rdd_filtered.map(data => data(0) + data(1));
//    import com.twitter.penguin.korean.TwitterKoreanProcessor
    val normalized = rdd.map( data => TwitterKoreanProcessor.normalize(data) )
    val tokens = normalized.flatMap(data => TwitterKoreanProcessor.tokenize(data))
    val tok_filtered = tokens.filter(d => (d.pos).toString contains("Noun"));
    val newsWords = tok_filtered.map(data => (data.text, 1.0));
    val newsWordsReduced = newsWords.reduceByKey(_+_);
    val newsSortedByValue = newsWordsReduced.map(item => item.swap).sortByKey(false, 1).map(item => item.swap);
    val newsWordsResult = newsSortedByValue.map{ case (k, v) => Array(k, v).mkString(", ")};
    // 매퍼 저장
    newsWordsResult.saveAsTextFile("/result_spark/news_meca_"+date);

    
    // predata에서 pre_gamenames.txt랑 game_weights.txt 합쳐서 게임 딕셔너리 만들기: (키,벨류) 매퍼 형태로
    val game1 = sc.textFile("/data/predata/pre_gamenames.txt");
    val game2 = sc.textFile("/data/predata/game_weights.txt");
    val gameresult = game1.union(game2)
    val gameresult2 = gameresult.map(data => data.split("\n"))
    val gameMap = gameresult2.map(data => data(0).split(","))
    val gameDic = gameMap.map(data => (data(0), data(1).toDouble))
    
    // 리듀싱 하기: newsWordsReduced: RDD[(String, Int)] & gameDic: RDD[(String, Double)]
    val resultRdd = newsWordsReduced.join(gameDic);
    val reducedRdd = resultRdd.map{case (k,v) => (k, v._1*v._2)}
    val reducedFilteredRdd = reducedRdd.filter{ case (k,v) => v != 1.0 }
    val reducedResult = reducedFilteredRdd.map{ case (k,v) => Array(k, v).mkString(", ")};
    reducedResult.saveAsTextFile("/result_spark/issues_news"+date);
    
  }
}