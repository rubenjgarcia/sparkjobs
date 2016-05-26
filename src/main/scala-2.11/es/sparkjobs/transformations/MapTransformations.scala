package es.sparkjobs.transformations

import spray.json._
import spray.json.DefaultJsonProtocol._

case class ReplaceAllMapTransformation(regexWithReplacement: (String, String)*) extends Transformation {
  @transient val formatter = ReplaceAllMapTransformation.formatter.asInstanceOf[JsonFormat[Transformation]]
  def unapply(s: String) = regexWithReplacement.foldLeft(s)((s, r) => s.replaceAll(r._1, r._2))
}

object ReplaceAllMapTransformation {
  val transformationType = "replaceAll"

  val formatter = new RootJsonFormat[ReplaceAllMapTransformation] {
    override def write(obj: ReplaceAllMapTransformation): JsValue = JsObject(
      "type" -> ReplaceAllMapTransformation.transformationType.toJson,
      "regex" -> obj.regexWithReplacement.foldLeft(List[String]())((list, r) => list :+ r._1).toJson,
      "replacement" -> obj.regexWithReplacement.foldLeft(List[String]())((list, r) => list :+ r._2).toJson
    )

    override def read(json: JsValue): ReplaceAllMapTransformation = {
      val regexs = json.asJsObject.fields.get("regex").get.asInstanceOf[JsArray].elements.map(_.convertTo[String])
      val replacements = json.asJsObject.fields.get("replacement").get.asInstanceOf[JsArray].elements.map(_.convertTo[String])
      ReplaceAllMapTransformation(regexs zip replacements: _*)
    }
  }
}

case class LowerCaseMapTransformation() extends Transformation {
  @transient val formatter = LowerCaseMapTransformation.formatter.asInstanceOf[JsonFormat[Transformation]]
  def unapply(s: String) = s.toLowerCase
}

object LowerCaseMapTransformation {
  val transformationType = "lowerCase"

  val formatter = new RootJsonFormat[LowerCaseMapTransformation] {
    override def write(obj: LowerCaseMapTransformation): JsValue = JsObject(
      "type" -> LowerCaseMapTransformation.transformationType.toJson
    )

    override def read(json: JsValue): LowerCaseMapTransformation = {
      LowerCaseMapTransformation()
    }
  }
}

case class TuplelizeMapTransformation[T, V](value: V) extends Transformation {
  @transient val formatter = TuplelizeMapTransformation.formatter.asInstanceOf[JsonFormat[Transformation]]
  def unapply(t: T) = (t, value)
}

object TuplelizeMapTransformation {
  val transformationType = "tuplelize"

  val formatter = new RootJsonFormat[TuplelizeMapTransformation[Any, Int]] {
    override def write(obj: TuplelizeMapTransformation[Any, Int]): JsValue = JsObject(
      "type" -> TuplelizeMapTransformation.transformationType.toJson,
      "value" -> obj.value.toJson
    )

    override def read(json: JsValue): TuplelizeMapTransformation[Any, Int] = {
      TuplelizeMapTransformation(json.asJsObject.fields.get("value").get.convertTo[Int])
    }
  }
}