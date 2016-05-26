package es.sparkjobs.transformations

import spray.json._
import spray.json.DefaultJsonProtocol._

abstract class FilterTransformation[T] extends Transformation {
  def filter(t: T) : Boolean
}

case class LengthFilterTransformation(length: Int) extends FilterTransformation[CharSequence] {
  @transient val formatter = LengthFilterTransformation.formatter.asInstanceOf[JsonFormat[Transformation]]
  override def filter(t: CharSequence) = t.length > length
}

object LengthFilterTransformation {
  val transformationType = "lengthFilter"

  val formatter = new RootJsonFormat[LengthFilterTransformation] {
    override def write(obj: LengthFilterTransformation): JsValue = JsObject(
      "type" -> LengthFilterTransformation.transformationType.toJson,
      "length" -> obj.length.toJson
    )

    override def read(json: JsValue): LengthFilterTransformation = LengthFilterTransformation(json.asJsObject.fields.get("length").get.asInstanceOf[JsNumber].value.intValue)
  }
}
