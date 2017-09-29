package com.springernature.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TestApp extends App {

  println("Started")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sc = new SparkContext("local[4]", "naivebayes")

  val textFileLoc = getClass.getResource("/test-file.txt")
  val lines: RDD[String] = sc.textFile(textFileLoc.getPath);
  println("Number of lines:" + lines.count)

  val words = lines.flatMap((line: String) => line.split(" "))
  println("Number of words:" + words.count)
  // Transform into pairs and count.
//  val counts: RDD[(String, Int)] = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  // Save the word count back out to a text file, causing evaluation.
//  counts.saveAsTextFile(outputFile)
//  counts.foreach(x => println(x._1  + ":" + x._2))



  println("Finished")

}
