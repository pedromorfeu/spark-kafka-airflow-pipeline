package com.pipeline

import com.pipeline.metadata.model.{DataflowMetadata, DataflowTransformationValidation}
import net.liftweb.json.{DefaultFormats, JValue}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, array_union, col, expr, lit, when}

import scala.io.Source

object PiepelineMain {

  val logger: Logger = Logger.getLogger(PiepelineMain.getClass)

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder
      .master("local[*]")
      .appName("Pipeline app")
      .getOrCreate()

    val metadataSource = Source.fromFile(args(0))
    val jsonStr = try metadataSource.mkString finally metadataSource.close()
    val liftJsonValue: JValue = net.liftweb.json.parse(jsonStr)

    implicit val formats: DefaultFormats.type = DefaultFormats // Brings in default date formats etc.
    val metadata = liftJsonValue.extract[DataflowMetadata]

    logger.info(metadata)

    metadata.dataflows
      .foreach(dataflow => {
        // dataflow processing
        logger.info(s"processing dataflow ${dataflow.name}")

        dataflow.sources.foreach(source => {
          logger.info(s"processing source ${source.name}")

          // source processing
          val inputPath = source.path
          logger.info(s"reading $inputPath")
          var dataframe = session.read.json(inputPath)
          dataframe.show

          // add fields
          dataflow.transformations
            .filter(_.`type` == "add_fields")
            .foreach(transformation => {
              logger.info(s"applying transformation ${transformation.name}")

              // TODO: use foldLeft: columns.foldLeft(df)((acc, col) => acc.withColumn(col, acc("incipit").as[String].contains(col)))
              transformation.params.addFields.get
                .foreach(field => dataframe = dataframe.withColumn(field.name, expr(field.function)))
            })

          // validate fields
          val conditions = dataflow.transformations
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

        })
      })
  }

}

