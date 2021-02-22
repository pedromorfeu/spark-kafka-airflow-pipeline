package com.pipeline

import com.pipeline.PipelineApp.logger
import com.pipeline.metadata.model._
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

        val transformations: Seq[DataflowTransformation] = dataflow.transformations
        val fieldsToAdd = transformations
          .filter(_.`type` == "add_fields")

        // read sources
        val sources = dataflow.sources
        val dataframe = readSources(session, sources)
          // add fields
          .transform(addFields(fieldsToAdd))

        // validate fields
        val fieldsToValidate = transformations
          .filter(_.`type` == "validate_fields")
        val (okDF, koDF) = validateFields(fieldsToValidate, dataframe)

        // persist
        persist(dataflow.sinks, okDF, koDF, kafkaServer)

      })
  }

  def validateFields(fieldsToValidate: Seq[DataflowTransformation], dataframe: DataFrame): (DataFrame, DataFrame) = {
    // validate fields
    val conditions = fieldsToValidate
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

    // caching dataframe as it will be used twice
    dataframe.cache

    logger.info(s"conditions: $conditions")
    val okDF = dataframe
      .filter(conditions.map(_._1).mkString(" and "))
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

    (okDF, notOkDF)
  }

  def addFields(fieldsToAdd: Seq[DataflowTransformation])(df: DataFrame): DataFrame = {
    var dataframe = df

    // add fields
    fieldsToAdd
      .foreach(transformation => {
        logger.info(s"applying transformation ${transformation.name}")

        // TODO: use foldLeft: columns.foldLeft(df)((acc, col) => acc.withColumn(col, acc("incipit").as[String].contains(col)))
        transformation.params.addFields.get
          .foreach(field => dataframe = dataframe.withColumn(field.name, expr(field.function)))
      })

    dataframe
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

  def persist(sinks: Seq[DataflowSink], okDF: DataFrame, koDF: DataFrame, kafkaServer: String): Unit =
    // persist
    sinks
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
            publishToKafka(okDF, kafkaServer, "pipeline-ok")

          case "validation_ko" =>
            sink.paths
              .foreach(path =>
                koDF
                  .write
                  .mode(sink.saveMode)
                  .format(sink.format)
                  .save(path + "/" + sink.name))

            // publish to kafka
            publishToKafka(koDF, kafkaServer, "pipeline-ko")
        }

      })

  def publishToKafka(okDF: DataFrame, kafkaServer: String, topicOk: String): Unit = {
    okDF
      .select(to_json(struct("*")).as("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", topicOk)
      .save()
  }

  def readMetadata(metadataPath: String): DataflowMetadata = {
    val metadataSource = Source.fromFile(metadataPath)
    val jsonStr = try metadataSource.mkString finally metadataSource.close()
    val liftJsonValue: JValue = net.liftweb.json.parse(jsonStr)

    implicit val formats: DefaultFormats.type = DefaultFormats // Brings in default date formats etc.
    val metadata = liftJsonValue.extract[DataflowMetadata]
    metadata
  }

}
