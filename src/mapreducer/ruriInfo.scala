package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.twitter.penguin.korean.TwitterKoreanProcessor

object ruriInfo {
   def main(args: Array[String]): Unit = {
    val sc = new SparkContext( new SparkConf().setAppName("InsightGameSpark"));
//    val file1 = sc.textFile("/ruri/3ds_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
//    var rdd = file1.filter(data => data(2) == date).map(data => data(0)+data(1))
    
    
    val file1 = sc.textFile("/ruri/3ds_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
//    val file2 = sc.textFile("/ruri/PC_info.txt");
    val file3 = sc.textFile("/ruri/PS4_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file4 = sc.textFile("/ruri/PSvista_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file5 = sc.textFile("/ruri/Switch_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file6 = sc.textFile("/ruri/Xbox_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
    val file7 = sc.textFile("/ruri/mobile_android_info.txt").flatMap(_.split("\n")).map(_.split("§§"));
//    var rdd = file4.flatMap(_.split("\n")).map(_.split("§§"));
//    rdd.zipWithIndex().foreach{case (item, index) => if( item.length > 3) println( s"$index => ", item(0),"["+item.length+"]")};
      
      
//    val fileSeq = Seq(file1, file2, file3, file4, file5, file6, file7);
    val fileSeq = Seq(file1, file3, file4, file5, file6, file7);
    val info = sc.union(Seq(file1, file3, file4, file5, file6, file7));
//    val infoRdd = info.flatMap(_.split("\n")).map(_.split("§§"));
    val infoRdd = info.filter(data => data.length == 3); 
    var date = java.time.LocalDate.now.toString;      
    if(args(0) != null || args(0) != "")
      date = args(0);
    val rdd_filtered = infoRdd.filter(data => data(2) == date);
    val rdd2 = rdd_filtered.map(data => data(0) + data(1));
    
    val normalized = rdd2.map( data => TwitterKoreanProcessor.normalize(data) )
    val tokens = normalized.flatMap(data => TwitterKoreanProcessor.tokenize(data))
    val tok_filtered = tokens.filter(d => (d.pos).toString contains("Noun"));
    val infoWords = tok_filtered.map(data => (data.text, 1.0));
    val infoWordsReduced = infoWords.reduceByKey(_+_);
    val infoSortedByValue = infoWordsReduced.map(item => item.swap).sortByKey(false, 1).map(item => item.swap);
    val infoWordsResult = infoSortedByValue.map{ case (k, v) => Array(k, v).mkString(", ")};
    // 매퍼 저장
    infoWordsResult.saveAsTextFile("/result_spark/users/info_"+date);

    
    // predata에서 pre_gamenames.txt랑 game_weights.txt 합쳐서 게임 딕셔너리 만들기: (키,벨류) 매퍼 형태로
    val game1 = sc.textFile("/data/predata/pre_gamenames.txt");
    val game2 = sc.textFile("/data/predata/game_weights.txt");
    val gameresult = game1.union(game2)
    val gameresult2 = gameresult.map(data => data.split("\n"))
    val gameMap = gameresult2.map(data => data(0).split(","))
    val gameDic = gameMap.map(data => (data(0), data(1).toDouble))
    
    // 리듀싱 하기: newsWordsReduced: RDD[(String, Int)] & gameDic: RDD[(String, Double)]
    val bcast = sc.broadcast(gameDic.map(_._1).collect());
    val infoFilteredBybcast = infoWordsReduced.filter(r => bcast.value.contains(r._1)); 
    val resultRdd = infoFilteredBybcast.join(gameDic);
    val reducedRdd = resultRdd.map{case (k,v) => (k, v._1*v._2)}
    val reducedFilteredRdd = reducedRdd.filter{ case (k,v) => v != 1.0 }
    val reducedResult = reducedFilteredRdd.map{ case (k,v) => Array(k, v).mkString(", ")};
    reducedResult.saveAsTextFile("/result_spark/issues/issues_info"+date);
    
   }
}