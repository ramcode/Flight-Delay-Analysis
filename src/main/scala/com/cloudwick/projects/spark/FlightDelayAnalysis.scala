package com.cloudwick.projects.spark

import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.network.sasl.SparkSaslServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.WeekOfYear
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by VenkataRamesh on 3/9/2017.
  */
object FlightDelayAnalysis {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      //.master("spark://master.cdh.yarn.cos7:7077")
      .master("local")
      .config("spark.sql.warehouse.dir", "src/main/res/Ontime")
      .appName("Flight Delay Analysis").getOrCreate()
    val sc = session.sparkContext
    val rdd2005 = sc.textFile("hdfs://master.cdh.yarn.cos7:8020/user/root/flight-data/Ontime/2005/2005.csv")
    //val rdd2005 = sc.textFile("file:///F:/Cloudwick-Training/Spark/Datasets/Ontime/2005/2005.csv")
    val header = rdd2005.first()
    val rdd2006 = sc.textFile("hdfs://master.cdh.yarn.cos7:8020/user/root/flight-data/Ontime/2006/2006.csv").filter(x => x != header)
    val rdd2007 = sc.textFile("hdfs://master.cdh.yarn.cos7:8020/user/root/flight-data/Ontime/2007/2007.csv").filter(x => x != header)
    val rdd2008 = sc.textFile("hdfs://master.cdh.yarn.cos7:8020/user/root/flight-data/Ontime/2008/2008.csv").filter(x => x != header)
    val dataRDD = rdd2005.filter(x => x != header)
    .union(rdd2006).union(rdd2007).union(rdd2008)
    val airports = sc.textFile("hdfs://master.cdh.yarn.cos7:8020/user/root/flight-data/Ontime/airports.csv")
    //val airports = sc.textFile("file:///F:/Cloudwick-Training/Spark/Datasets/Ontime/airports.csv")
    //val carriers = sc.textFile("file:///F:/Cloudwick-Training/Spark/Datasets/Ontime/carriers.csv")
    val carriers = sc.textFile("hdfs://master.cdh.yarn.cos7:8020/user/root/flight-data/Ontime/carriers.csv")
    val city = ConfigFactory.load().getString("flights.delay.city")
    val airport = ConfigFactory.load().getString("flights.delay.airport")
    println(s"Entered City: $city")
    println(s"Entered airport: $airport")
    //case classes
    case class AirportData(iata: String,
                           airport: String,
                           city: String,
                           state: String,
                           country: String)
    case class CarrierData(code: String,
                           description: String)
    case class FlightData(year: String,
                          month: Int,
                          dayOfMonth: Int,
                          dayOfWeek: Int,
                          weekOfYear: Int,
                          uniqueCarrier: String,
                          flightNum: String,
                          arrDelay: Float,
                          depDelay: Float,
                          origin: String,
                          dest: String
                         )

    //utility functions
    def trimQuotes(str: String): String = {
      val modStr = str.replaceAll("^\"|\"$", "")
      modStr
    }
    def getWeekOfYear(year: Int, month: Int, dayOFMonth: Int): Int = {
      val cal = Calendar.getInstance()
      cal.set(year, month, dayOFMonth)
      cal.get(Calendar.WEEK_OF_YEAR)
    }
    def filterNA(delay: String): Float = {
      if (delay == "NA") 0
      else delay.toFloat
    }
    //
    //cleaning carrier and airport data
    val airportHdr = airports.first()
    val airportRDD = airports.filter(x => x != airportHdr).map(x => x.split(",")).map(x => AirportData(trimQuotes(x(0)), trimQuotes(x(1)), trimQuotes(x(2)), trimQuotes(x(3)), trimQuotes(x(4))))

    val carrierHDr = carriers.first()
    val carrierRDD = carriers.filter(x => x != carrierHDr).map(x => x.split(",")).map(x => CarrierData(trimQuotes(x(0)), trimQuotes(x(1))))
    //creating case rdd
    val mappedRDD = dataRDD.map(x => x.split(",")).map(x => FlightData(x(0), x(1).toInt, x(2).toInt, x(3).toInt, getWeekOfYear(x(0).toInt, x(1).toInt, x(2).toInt), x(8), x(9), filterNA(x(14)), filterNA(x(15)), x(16), x(17)))
    val filteredRDD = mappedRDD.filter(x => {
      (x.origin == airport || x.dest == airport) && (x.arrDelay > 0 || x.depDelay > 0)
    })
    filteredRDD.take(20).foreach(println)
    //group by year and week
    val groupedRDD = filteredRDD.groupBy(x => (x.year, x.weekOfYear)).map(x => (x._1, x._2.groupBy(x => x.uniqueCarrier)))
    val finalRDD = groupedRDD.mapValues(x => x.map(x => (x._1, {
      var delay = 0f
      x._2.foreach(x => {
        if (x.arrDelay > 0)
          delay += x.depDelay
        if (x.depDelay > 0) delay += x.depDelay
      })
      delay
    }))).sortBy(x => x._1).map(x => (x._1._1, x._1._2, x._2.mkString(","))).coalesce(1,true).saveAsTextFile("hdfs://master.cdh.yarn.cos7:8020/user/root/data/output/")
  }

}
