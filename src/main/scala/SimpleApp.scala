/**
  * Created by liuxingqi on 16/6/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object SimpleApp {

  // 获取某个单词的数量
  def get_word_count(rDD: RDD[String], word: String): Unit = {
    val numspark = rDD.filter(line => line.contains(word)).count()
    println("number of %s is %s".format(word, numspark))
  }

  // 获取文本行数
  def get_lines_count(rDD: RDD[String]): Long = {
    rDD.count()
  }

  // 获取所有单词数
  def get_words_count(rDD: RDD[String]): Long = {
    rDD.flatMap(line => line.split(" ")).count()
  }

  // 获取单词键值对并计数
  def get_words_map(rDD: RDD[String]): collection.Map[String, Long] = {
    rDD.flatMap(line => line.split(" ")).countByValue()
  }

  def main(args: Array[String]) {
    val logFile = "/Users/liuxingqi/spark/spark-1.6.1/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val words_map = get_words_map(logData)
    println("words map is %s", words_map)
  }
}
