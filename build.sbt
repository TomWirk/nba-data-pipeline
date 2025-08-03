// Nom du projet
name := "NBA"

version := "1.0"

scalaVersion := "2.12.15"

// Dépendances
libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.1.8",
  "com.lihaoyi" %% "ujson" % "0.7.1",
//  "com.lihaoyi" %% "os-lib" % "0.7.8",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
