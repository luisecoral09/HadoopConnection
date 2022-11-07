package com.sparkdemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object demo1 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    val sc = new SparkContext("local[*]", "SparkDemo01")
    val lines = sc.textFile("C:\\Users\\Consultant\\Downloads\\simple_test.txt")
    val words = lines.flatMap(line => line.split(' '))
    val wordsKVRdd = words.map(x => (x, 1))
    val count = wordsKVRdd.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)

    count.foreach(println)
    //val df2 = sc.objectFile("C:\\Users\\Consultant\\Downloads\\input_1.csv")
    //val df1 = sc.read.Option("header","true").csv("C:\\Users\\Consultant\\Downloads\\input_1")
    //val df = df1.unionByName(df2)
    //println(df2.toString())
  }
}
