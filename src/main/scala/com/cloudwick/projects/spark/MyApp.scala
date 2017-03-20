package com.cloudwick.projects.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ${user.name}
  */
object MyApp extends App {
  val conf = new SparkConf()
  conf.setAppName("WordCount")
  val sc = new SparkContext(conf)
  val textFile = sc.textFile("file:///input/carriers.csv")
  val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  counts.saveAsTextFile("file:///output/")

}
