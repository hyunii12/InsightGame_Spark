package mapreducer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ruriContents {
   def ltrim(s: String) = s.replaceAll("^\\s+", "")
   def rtrim(s: String) = s.replaceAll("\\s+$", "")
   def main(args: Array[String]): Unit = {
    val sc = new SparkContext( new SparkConf().setAppName("InsightGameSpark"));
    val file1 = sc.textFile("/ruri/3ds_info.txt").flatMap(_.split("\n")).map(_.split(","));
    var date = java.time.LocalDate.now.toString;      
    if(args(0) != null || args(0) != "")
      date = args(0);
    var rdd = file1.filter(data => data(2) == date).map(data => data(0)+data(1))
    val file2 = sc.textFile("/ruri/PC_info.txt");
    val file3 = sc.textFile("/ruri/PS4_info.txt");
    val file4 = sc.textFile("/ruri/PSvista_info.txt");
    val file5 = sc.textFile("/ruri/Switch_info.txt");
    val file6 = sc.textFile("/ruri/Xbox_info.txt");
    val file7 = sc.textFile("/ruri/mobile_android_info.txt");
   }
}