import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  lazy val mockito = "org.mockito" % "mockito-all" % "1.9.5" % "test"
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
  lazy val sparkMlLib = "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"
  lazy val sparkStreaming = "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided"
  lazy val akka =  "com.typesafe.akka" %% "akka-actor" % "2.5.4"
  lazy val akkaTest = "com.typesafe.akka" %% "akka-testkit" % "2.5.4"
  lazy val akkaRemote = "com.typesafe.akka" % "akka-remote_2.11" % "2.5.4"
  lazy val sl4jApi = "org.slf4j" % "slf4j-api" % "1.7.5"
  lazy val sl4jSimple = "org.slf4j" % "slf4j-simple" % "1.7.5"
  lazy val scalaCsv = "com.github.tototoshi" %% "scala-csv" % "1.3.5"
}
