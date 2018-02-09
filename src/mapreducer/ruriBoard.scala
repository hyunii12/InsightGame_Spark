package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.twitter.penguin.korean.TwitterKoreanProcessor

object ruriBoard {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    val file1 = sc.textFile("/ruri/3ds_board.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file2 = sc.textFile("/ruri/PC_board.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file3 = sc.textFile("/ruri/PS4_board.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file4 = sc.textFile("/ruri/PSvista_board.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file5 = sc.textFile("/ruri/Switch_board.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file6 = sc.textFile("/ruri/Xbox_board.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file7 = sc.textFile("/ruri/mobile_board.txt").flatMap(_.split("\n")).map(_.split("§§"));

    val board = sc.union(Seq(file1, file2, file3, file4, file5, file6, file7));
    //    val boardRdd = board.flatMap(_.split("\n")).map(_.split("§§"));
    val boardRdd = board.filter(data => data.length == 3);
    var date = java.time.LocalDate.now.toString;
    if (args(0) != null || args(0) != "")
      date = args(0);
    val rdd_filtered = boardRdd.filter(data => data(2) == date);
    val rdd2 = rdd_filtered.map(data => data(0) + data(1));

    val normalized = rdd2.map(data => TwitterKoreanProcessor.normalize(data));
    val tokens = normalized.flatMap(data => TwitterKoreanProcessor.tokenize(data));
    val tok_filtered = tokens.filter(d => (d.pos).toString contains ("Noun"));
    val boardWords = tok_filtered.map(data => (data.text, 1.0));
    val boardWordsReduced = boardWords.reduceByKey(_ + _);
    val boardSortedByValue = boardWordsReduced.map(item => item.swap).sortByKey(false, 1).map(item => item.swap);
    val boardWordsResult = boardSortedByValue.map { case (k, v) => Array(k, v).mkString(", ") };
    // 매퍼 저장
    boardWordsResult.saveAsTextFile("/result_spark/users/board_" + date);

    // predata에서 pre_gamenames.txt랑 game_weights.txt 합쳐서 게임 딕셔너리 만들기: (키,벨류) 매퍼 형태로
    val game1 = sc.textFile("/data/predata/pre_gamenames.txt");
    val game2 = sc.textFile("/data/predata/game_weights.txt");
    val gameresult = game1.union(game2);
    val gameresult2 = gameresult.map(data => data.split("\n"));
    val gameMap = gameresult2.map(data => data(0).split(","));
    val gameDic = gameMap.map(data => (data(0), data(1).toDouble));

    // 리듀싱 하기: newsWordsReduced: RDD[(String, Int)] & gameDic: RDD[(String, Double)]
    val bcast = sc.broadcast(gameDic.map(_._1).collect());
    val boardFilteredBybcast = boardWordsReduced.filter(r => bcast.value.contains(r._1));
    val resultRdd = boardFilteredBybcast.join(gameDic);
    val reducedRdd = resultRdd.map { case (k, v) => (k, v._1 * v._2) };
    val reducedFilteredRdd = reducedRdd.filter { case (k, v) => v != 1.0 };
    val reducedResult = reducedFilteredRdd.map { case (k, v) => Array(k, v).mkString(", ") };
    reducedResult.saveAsTextFile("/result_spark/issues/issues_board" + date);

  }
}