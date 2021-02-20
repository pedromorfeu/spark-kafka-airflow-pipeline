package com.pipeline

import com.pipeline.PiepelineMain.logger
import com.pipeline.metadata.model.{Dataflow, DataflowMetadata, DataflowSource, DataflowTransformationValidation}
import net.liftweb.json.{DefaultFormats, JValue}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

class PipelineProcessor(session: SparkSession, metadataPath: String, kafkaServer: String) {

  def process(): Unit = {
    // read metadata
    val metadata: DataflowMetadata = readMetadata(metadataPath)
    logger.info(metadata)

    metadata.dataflows
      .foreach(dataflow => {
        // dataflow processing
        logger.info(s"processing dataflow ${dataflow.name}")

        // read sources
        val sources = dataflow.sources
        var dataframe = readSources(session, sources)

        // add fields
        val transformations = dataflow.transformations
        transformations
          .filter(_.`type` == "add_fields")
          .foreach(transformation => {
            logger.info(s"applying transformation ${transformation.name}")

            // TODO: use foldLeft: columns.foldLeft(df)((acc, col) => acc.withColumn(col, acc("incipit").as[String].contains(col)))
            transformation.params.addFields.get
              .foreach(field => dataframe = dataframe.withColumn(field.name, expr(field.function)))
          })

        // validate fields
        val conditions = transformations
          .filter(_.`type` == "validate_fields")
          .flatMap(transformation => {
            logger.info(s"applying transformation ${transformation.name}")

            // transformation
            logger.info("validating fields")
            val validations: List[DataflowTransformationValidation] = transformation.params.validations.get

            validations
              .flatMap(validation => {
                validation.validations
                  .map {
                    case "notEmpty" => (s"${validation.field} != ''", s"${validation.field}-notEmpty")
                    case "notNull" => (s"${validation.field} is not null", s"${validation.field}-notNull")
                  }
              })
          })

        logger.info(s"conditions: $conditions")
        val okDF = dataframe.filter(conditions.map(_._1).mkString(" and "))
        okDF.show(false)

        var notOkDF = dataframe
          .filter(s"!(${conditions.map(_._1).mkString(" and ")})")
          .withColumn("arraycoderrorbyfield", array())

        conditions
          .foreach(condition => {
            notOkDF = notOkDF
              .withColumn("arraycoderrorbyfield",
                when(expr(s"!${condition._1}"), array_union(col("arraycoderrorbyfield"), lit(Array(condition._2))))
                  .otherwise(col("arraycoderrorbyfield")))
          })

        notOkDF.show(false)

        // persist
        persist(dataflow, okDF, notOkDF, kafkaServer)

      })
  }

  def readSources(session: SparkSession, sources: List[DataflowSource]): DataFrame =
    sources
      .map(source => {
        logger.info(s"source ${source.name}")
        val inputPath = source.path
        logger.info(s"reading $inputPath")
        session.read.format(source.format).load(inputPath)
      })
      .reduce(_ union _)

  def persist(dataflow: Dataflow, okDF: DataFrame, notOkDF: DataFrame, kafkaServer: String): Unit =
    // persist
    dataflow.sinks
      .foreach(sink => {

        sink.input match {
          case "ok_with_date" =>
            sink.paths
              .foreach(path =>
                okDF
                  .write
                  .mode(sink.saveMode)
                  .format(sink.format)
                  .save(path + "/" + sink.name))

            // publish to kafka
            okDF
              .select(to_json(struct("*")).as("value"))
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaServer)
              .option("topic", "pipeline-ok")
              .save()

          case "validation_ko" =>
            sink.paths
              .foreach(path =>
                notOkDF
                  .write
                  .mode(sink.saveMode)
                  .format(sink.format)
                  .save(path + "/" + sink.name))

            // publish to kafka
            notOkDF
              .select(to_json(struct("*")).as("value"))
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaServer)
              .option("topic", "pipeline-ko")
              .save()
        }

      })

  def readMetadata(metadataPath: String): DataflowMetadata = {
    val metadataSource = Source.fromFile(metadataPath)
    val jsonStr = try metadataSource.mkString finally metadataSource.close()
    val liftJsonValue: JValue = net.liftweb.json.parse(jsonStr)

    implicit val formats: DefaultFormats.type = DefaultFormats // Brings in default date formats etc.
    val metadata = liftJsonValue.extract[DataflowMetadata]
    metadata
  }

}
