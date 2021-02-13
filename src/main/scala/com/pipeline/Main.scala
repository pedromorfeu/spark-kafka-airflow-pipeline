package com.pipeline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.pipeline.metadata.model.DataflowMetadata
import net.liftweb.json.{DefaultFormats, JValue}

import scala.io.Source

object Main {

  def main(args: Array[String]): Unit = {

//    val session = SparkSession
//      .builder
//      .master("local[*]")
//      .appName("Simple Application")
//      .getOrCreate()
//    session.sparkContext.setLogLevel("ERROR")
//
//    session.sparkContext.parallelize(Array(1, 2, 3))
//      .map(_ * 2)
//      .foreach(println)

    val json = Source.fromFile("/Users/pedromorfeu/IdeaProjects/spark-kafka-airflow-pipeline/src/main/resources/metadata.json")
    // parse
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val parsedJson = mapper.readValue[Map[String, Object]](json.reader())
    println(parsedJson)

    println(parsedJson("dataflows"))

    val jsonStr = Source.fromFile("/Users/pedromorfeu/IdeaProjects/spark-kafka-airflow-pipeline/src/main/resources/metadata.json").mkString
    val liftJsonValue: JValue = net.liftweb.json.parse(jsonStr)

    println(liftJsonValue)

    implicit val formats: DefaultFormats.type = DefaultFormats // Brings in default date formats etc.
    val metadata = liftJsonValue.extract[DataflowMetadata]
    println(metadata.dataflows.head)

    //    val jsonString = os.read(os.root)
    //    val data: Value = ujson.read(jsonString)
    //    println(data("dataflows"))
    //
    //    val inputPath = data("dataflows")(0)("sources")(0)("path").toString
    //    println(inputPath)

//    session.read.json(inputPath).show

  }

}
