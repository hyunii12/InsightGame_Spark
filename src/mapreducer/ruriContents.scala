package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.twitter.penguin.korean.TwitterKoreanProcessor

object ruriContents {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("InsightGameSpark"));
    var file1 = sc.textFile("hdfs://h1:9000/ruri/3ds_board.txt") 
    var file2 = sc.textFile("hdfs://h1:9000/ruri/PC_board.txt") 
    var file3 = sc.textFile("hdfs://h1:9000/ruri/PS4_board.txt") 
    var file4 = sc.textFile("hdfs://h1:9000/ruri/PSvista_board.txt") 
    var file5 = sc.textFile("hdfs://h1:9000/ruri/Switch_board.txt") 
    var file6 = sc.textFile("hdfs://h1:9000/ruri/Xbox_board.txt") 
    var file7 = sc.textFile("hdfs://h1:9000/ruri/mobile_board.txt") 
    val board = sc.union(Seq(file1, file2, file3, file4, file5, file6, file7));
    var boardRdd = board.flatMap(_.split("\n")).map(_.split("§§"));
    
    var file_1 = sc.textFile("hdfs://h1:9000/ruri/3ds_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_2 = sc.textFile("hdfs://h1:9000/ruri/PC_info.txt").flatMap(_.split("\n")).map(_.split("§§★§★§§"));;
    var file_3 = sc.textFile("hdfs://h1:9000/ruri/PS4_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_4 = sc.textFile("hdfs://h1:9000/ruri/PSvista_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_5 = sc.textFile("hdfs://h1:9000/ruri/Switch_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_6 = sc.textFile("hdfs://h1:9000/ruri/Xbox_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    var file_7 = sc.textFile("hdfs://h1:9000/ruri/mobile_android_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
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
    val tok_filtered = tokens.filter(d => {(d.pos).toString contains "Noun"} 
      || {(d.pos).toString contains "Number"}
      || {(d.pos).toString contains "Alpha"});
//    val tok_filtered = tokens.filter(d => (d.pos).toString contains ("Noun", "Number", "Alpha"));
    val ruriWords = tok_filtered.map(data => (data.text, 1.0));
    val ruriWordsReduced = ruriWords.reduceByKey(_ + _).filter(_._2 > 1.0);
    val ruriSortedByValue = ruriWordsReduced.map(item => item.swap).sortByKey(false, 1).map(item => item.swap);
    val ruriWordsResult = ruriSortedByValue.map { case (k, v) => Array(k, v).mkString(", ") };
    // 매퍼 저장
    ruriWordsResult.saveAsTextFile("/result_spark/users/ruri_contents_" + date);  }
}