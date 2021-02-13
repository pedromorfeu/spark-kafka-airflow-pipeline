name := "spark-kafka-airflow-pipeline"

version := "0.1"

scalaVersion := "2.12.10"

// spark
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1" % Provided

// json
libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.0"

mainClass in assembly := Some("com.pipeline.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}