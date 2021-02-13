package com.pipeline.metadata.model

case class DataflowMetadata(dataflows: List[Dataflow])

case class Dataflow(name: String,
                    sources: List[DataflowSource],
                    transformations: List[DataflowTransformation],
                    sinks: List[DataflowSink]
                   )

case class DataflowSource(name: String,
                          path: String,
                          format: String)

case class DataflowTransformation(name: String,
                                  `type`: String,
                                  params: DataflowTransformationParam)

case class DataflowTransformationParam(input: String,
                                       validations: List[DataflowTransformationValidation],
                                       addFields: List[DataflowTransformationAddField])

case class DataflowTransformationValidation(field: String,
                                            validations: List[String])

case class DataflowTransformationAddField(name: String,
                                          function: String)

case class DataflowSink(input: String,
                        name: String,
                        paths: List[String],
                        format: String,
                        saveMode: String)



