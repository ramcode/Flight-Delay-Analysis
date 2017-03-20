package com.cloudwick.projects.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by VenkataRamesh on 3/15/2017.
  */
object WashingtonCrimeDataAnalysis {



  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .config("spark.sql.warehouse.dir", "src/main/res/Ontime")
      .appName("Flight Delay Analysis").getOrCreate()
    val sc = session.sparkContext
    val rdd2005 = session.sparkContext.textFile("file:///F:/Cloudwick-Training/Spark/Spark-Assignment/src/main/res/Ontime/2005/wash_dc_crime_incidents_2013.csv")
    println(s"Count Including header ${rdd2005.count()}")
    //rdd2005.take(100).foreach(x=>println(x))
    val header = rdd2005.first()
    println(header)
    val rdd2005NH = rdd2005.filter(x => x != header)
    rdd2005NH.take(100).foreach(x=>println(x))
    case class CrimeData(ccn: String,
                         reportTime: String,
                         shift: String,
                         offense: String,
                         method: String)
    val caseRDD = rdd2005NH.map(x=>x.split(",")).map(x=>CrimeData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString))
    caseRDD.take(10).foreach(x=>println(x))
    val cachedRDD = caseRDD.cache()
    cachedRDD.take(10).foreach(x=>println(x))
    val groupedRDD = cachedRDD.groupBy(x=>x.offense).map(x=>(x._1,x._2.size))
    groupedRDD.foreach(x=>println(x))
    val homicideGrp = cachedRDD.filter(x=>x.offense=="HOMICIDE").groupBy(x=>x.method).map(x=>(x._1,x._2.size))
    homicideGrp.foreach(x=>println(x))
    println(s"Count Excluding header ${rdd2005NH.count()}")

    //data frames
    val sqlContext = session.sqlContext
    val peopleRDD = sqlContext.read.textFile("file:///F:/Cloudwick-Training/Spark/Spark-Assignment/src/main/res/Ontime/2005/people.txt")
    println(peopleRDD.show(10))
    case class Person(firstName: String,
                      middleName: String,
                      lastName:   String,
                      gender:     String,
                      birthDate:  String,
                      salary:     String,
                      ssn:        String)
    //val hdr = df.head()
    import sqlContext.implicits._
    //val peopleRDD = df.filter(x=>x!=hdr)
    //println(filterdDf.show(10))
    //val peopleRDD = filterdDf.map(x=>x.split(":")).map(x=>Person(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString))
    //val peopleDf = peopleRDD.toDF()





  }
}

