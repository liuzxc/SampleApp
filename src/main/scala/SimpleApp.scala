/**
  * Created by liuxingqi on 16/6/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/liuxingqi/meiqia/ecoboost/spark-1.6.1/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numspark = logData.filter(line => line.contains("spark")).count()
    println("Lines with spark: %s".format(numspark))
  }
}
