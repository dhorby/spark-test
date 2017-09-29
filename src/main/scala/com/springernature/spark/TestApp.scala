package com.springernature.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
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

  // Parse the data
  val parsedData = lines.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

  // Cluster the data into two classes using KMeans
  val numClusters = 2
  val numIterations = 20
  val clusters = KMeans.train(parsedData, numClusters, numIterations)

  // Evaluate clustering by computing Within Set Sum of Squared Errors
  val WSSSE = clusters.computeCost(parsedData)
  println("Within Set Sum of Squared Errors = " + WSSSE)

  // Save and load model
//  clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
//  val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")



  println("Finished")

}
