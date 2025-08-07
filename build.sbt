// Nom du projet
name := "NBA"

version := "1.0"

scalaVersion := "2.12.15" // Compatible avec Spark 3.5.x

// Dépendances principales
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0", // Tu avais oublié spark-core
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
)

// Dépendances utilitaires (facultatives)
libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.1.8",
  "com.lihaoyi" %% "ujson" % "0.7.1"
)
