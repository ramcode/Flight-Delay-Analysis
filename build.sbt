name := "flight-delay-analysis"
version := "1.0"
scalaVersion := "2.11.8"
assemblyJarName in assembly := "flight-delay-analysis.jar"
autoScalaLibrary := false
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
exportJars := true


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"

resolvers += "MavenRepository" at "https://mvnrepository.com/"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"              => MergeStrategy.concat
  case x =>
    val baseStrategy = (assemblyMergeStrategy in assembly).value
    baseStrategy(x)
}

