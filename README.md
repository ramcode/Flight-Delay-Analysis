# Flight-Delay-Analysis
Spark project for analyzing flight delays for US flight data

Step to run jar using spark-submit

1. Edit application.conf to configure input parameters like airport code, city etc
2. give location of application.conf as jar argument as specified below

/bin/spark-submit --master spark://master.cdh.yarn.cos7:7077 \
--class com.cloudwick.projects.spark.FlightDelayAnalysis \
spark-assignment.jar application.conf

Output will be generated in hdfs://master.cdh.yarn.cos7:8020/user/root/data/output/

Output format will be as follows

Year, Week, CSV of (carrier->delay)
(2005,1,DL -> 72.5,F9 -> 16.183332,US -> 35.766666,OO -> 495.16666,TZ -> 37.65,HP -> 77.183334,AA -> 258.03333,NW -> 67.916664,FL -> 9.333333,CO -> 89.46667,UA -> 571.23334,MQ -> 20.583334,AS -> 84.833336,HA -> 0.95)


