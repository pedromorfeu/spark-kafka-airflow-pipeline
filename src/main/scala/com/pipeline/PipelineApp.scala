package com.pipeline

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object PipelineApp {

  val logger: Logger = Logger.getLogger(PipelineApp.getClass)

  def main(args: Array[String]): Unit = {

    val metadataPath = args(0)
    val kafkaServer = args(1)

    // Spark session
    val session = SparkSession
      .builder
      .appName("Pipeline app")
      .getOrCreate()

    new PipelineProcessor(session, metadataPath, kafkaServer).process()

  }
}

