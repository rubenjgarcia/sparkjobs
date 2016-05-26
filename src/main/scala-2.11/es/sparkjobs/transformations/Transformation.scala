package es.sparkjobs.transformations

import spray.json._

//  val mapPartitions = 3
//  val mapPartitionsWithIndex = 4
//  val sample = 5
//  val union = 6
//  val intersection = 7
//  val distinct = 8
//  val groupByKey = 9
//  val aggregateByKey = 11
//  val join = 13
//  val cogroup = 14
//  val cartesian = 15
//  val pipe = 16
//  val coalesce = 17
//  val repartition = 18
//  val repartitionAndSortWithinPartitions = 19

trait Transformation extends Serializable {
  val formatter: JsonFormat[Transformation]
}

object Transformation {
  case class TransformationNotRegisteredException(msg: String) extends Exception(msg)

  val transformations = scala.collection.mutable.HashMap.empty[String, JsonFormat[Transformation]]

  def register[T <: Transformation](transformationType: String, formatter: JsonFormat[T]) : Unit = {
    transformations += (transformationType -> formatter.asInstanceOf[JsonFormat[Transformation]])
  }

  Transformation.register(LengthFilterTransformation.transformationType, LengthFilterTransformation.formatter)

  Transformation.register(SortByKeyTransformation.transformationType, SortByKeyTransformation.formatter)
  Transformation.register(SortByValueTransformation.transformationType, SortByValueTransformation.formatter)

  Transformation.register(ReplaceAllMapTransformation.transformationType, ReplaceAllMapTransformation.formatter)
  Transformation.register(LowerCaseMapTransformation.transformationType, LowerCaseMapTransformation.formatter)

  Transformation.register(SplitFlatMapTransformation.transformationType, SplitFlatMapTransformation.formatter)
  Transformation.register(TuplelizeMapTransformation.transformationType, TuplelizeMapTransformation.formatter)

  Transformation.register(ReduceByKeySumTransformation.transformationType, ReduceByKeySumTransformation.formatter)

  implicit val formatter = new JsonFormat[Transformation] {
    override def write(obj: Transformation): JsValue = obj.formatter.write(obj)

    override def read(json: JsValue): Transformation = {
      val jsonTransformationType = json.asJsObject.fields.get("type").get.asInstanceOf[JsString].value
      val transformationFormatter = transformations.get(jsonTransformationType)
      if (transformationFormatter.isEmpty) {
        throw new TransformationNotRegisteredException(s"There is no formatter for transformation of type $jsonTransformationType")
      } else {
        transformationFormatter.get.read(json)
      }
    }
  }
}
