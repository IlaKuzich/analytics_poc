name := "analytics_processing"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "3.0.1"
val postgresConnector = "42.2.14"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion % "provided"
libraryDependencies += "org.postgresql" % "postgresql" % postgresConnector  % "provided"