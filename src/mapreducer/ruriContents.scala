package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.twitter.penguin.korean.TwitterKoreanProcessor

object ruriContents {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    var file1 = sc.textFile("/ruri/3ds_board.txt") 
    var file2 = sc.textFile("/ruri/PC_board.txt") 
    var file3 = sc.textFile("/ruri/PS4_board.txt") 
    var file4 = sc.textFile("/ruri/PSvista_board.txt") 
    var file5 = sc.textFile("/ruri/Switch_board.txt") 
    var file6 = sc.textFile("/ruri/Xbox_board.txt") 
    var file7 = sc.textFile("/ruri/mobile_board.txt") 
    val board = sc.union(Seq(file1, file2, file3, file4, file5, file6, file7));
    var boardRdd = board.flatMap(_.split("\n")).map(_.split("§§"));
    
    var file_1 = sc.textFile("/ruri/3ds_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_2 = sc.textFile("/ruri/PC_info.txt").flatMap(_.split("\n")).map(_.split("§§★§★§§"));;
    var file_3 = sc.textFile("/ruri/PS4_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_4 = sc.textFile("/ruri/PSvista_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_5 = sc.textFile("/ruri/Switch_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_6 = sc.textFile("/ruri/Xbox_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_7 = sc.textFile("/ruri/mobile_android_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val info = sc.union(Seq(file_1, file_2, file_3, file_4, file_5, file_6, file_7));
    val ruri = boardRdd.union(info);
    
    val rdd = ruri.filter(data => data.length == 3);
    var date = java.time.LocalDate.now.toString;
    if (args(0) != null || args(0) != "")
      date = args(0);
    val rdd_filtered = rdd.filter(data => data(2) == date);
    val rdd2 = rdd_filtered.map(data => data(0) + data(1));

    val normalized = rdd2.map(data => TwitterKoreanProcessor.normalize(data))
    val tokens = normalized.flatMap(data => TwitterKoreanProcessor.tokenize(data))
    val tok_filtered = tokens.filter(d => (d.pos).toString contains ("Noun"));
    val ruriWords = tok_filtered.map(data => (data.text, 1.0));
    val ruriWordsReduced = ruriWords.reduceByKey(_ + _);
    val ruriSortedByValue = ruriWordsReduced.map(item => item.swap).sortByKey(false, 1).map(item => item.swap);
    val ruriWordsResult = ruriSortedByValue.map { case (k, v) => Array(k, v).mkString(", ") };
    // 매퍼 저장
    ruriWordsResult.saveAsTextFile("/result_spark/users/ruri_" + date);

    // predata에서 pre_gamenames.txt랑 game_weights.txt 합쳐서 게임 딕셔너리 만들기: (키,벨류) 매퍼 형태로
    val game1 = sc.textFile("/data/predata/pre_gamenames.txt");
    val game2 = sc.textFile("/data/predata/game_weights.txt");
    val gameresult = game1.union(game2)
    val gameresult2 = gameresult.map(data => data.split("\n"))
    val gameMap = gameresult2.map(data => data(0).split(","))
    val gameDic = gameMap.map(data => (data(0), data(1).toDouble))

    // 리듀싱 하기: ruriWordsReduced: RDD[(String, Int)] & gameDic: RDD[(String, Double)]
    val bcast = sc.broadcast(gameDic.map(_._1).collect());
    val ruriFilteredBybcast = ruriWordsReduced.filter(r => bcast.value.contains(r._1));
    val resultRdd = ruriFilteredBybcast.join(gameDic);
    val reducedRdd = resultRdd.map { case (k, v) => (k, v._1 * v._2) }
    val reducedFilteredRdd = reducedRdd.filter { case (k, v) => v != 1.0 }.map(item => item.swap).sortByKey(false, 1).map(item => item.swap);
    val reducedResult = reducedFilteredRdd.map { case (k, v) => Array(k, v).mkString(", ") };
    reducedResult.saveAsTextFile("/result_spark/issues/issues_ruri" + date);
  }
}