ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "flink-examples"
  )




// https://mvnrepository.com/artifact/org.apache.flink/flink-core
libraryDependencies += "org.apache.flink" % "flink-core" % "1.16.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.16.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.1"



