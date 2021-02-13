package com.pipeline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.pipeline.metadata.model.DataflowMetadata
import net.liftweb.json.{DefaultFormats, JValue}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object Main {

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder
      .master("local[*]")
      .appName("Simple Application")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    session.sparkContext.parallelize(Array(1, 2, 3))
      .map(_ * 2)
      .foreach(println)

    val jsonStr = Source.fromFile("/Users/pedromorfeu/IdeaProjects/spark-kafka-airflow-pipeline/src/main/resources/metadata.json").mkString
    val liftJsonValue: JValue = net.liftweb.json.parse(jsonStr)

    implicit val formats: DefaultFormats.type = DefaultFormats // Brings in default date formats etc.
    val metadata = liftJsonValue.extract[DataflowMetadata]

    println(metadata)

    val inputPath = metadata.dataflows.head.sources.head.path
    println(inputPath)
    println(inputPath)

    session.read.json(inputPath).show

  }

}
