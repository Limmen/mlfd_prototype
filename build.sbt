import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "kth.se.ii2202.mlfd_prototype",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "mlfd_prototype",
    libraryDependencies ++= Seq(
      scalaTest,
      mockito,
      sparkCore,
      sparkSql,
      sparkMlLib,
      sparkStreaming,
      akka,
      akkaTest,
      scalaTest,
      akkaRemote,
      sl4jApi,
      sl4jSimple,
      scalaCsv,
      scallop
    )
  )
