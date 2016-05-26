package es.sparkjobs.transformations

import spray.json._
import spray.json.DefaultJsonProtocol._

case class SplitFlatMapTransformation(regex: String) extends Transformation {
  @transient val formatter = SplitFlatMapTransformation.formatter.asInstanceOf[JsonFormat[Transformation]]
  def unapply(s: String) = s.split(regex)
}

object SplitFlatMapTransformation {
  val transformationType = "split"

  val formatter = new RootJsonFormat[SplitFlatMapTransformation] {
    override def write(obj: SplitFlatMapTransformation): JsValue = JsObject(
      "type" -> SplitFlatMapTransformation.transformationType.toJson,
      "regex" -> obj.regex.toJson
    )

    override def read(json: JsValue): SplitFlatMapTransformation = SplitFlatMapTransformation(json.asJsObject.fields.get("regex").get.asInstanceOf[JsString].value)
  }
}
