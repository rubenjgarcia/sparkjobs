package es.sparkjobs.rdds

import spray.json.DefaultJsonProtocol._
import spray.json._

case class TextFileRDD(path: String) extends RDD {
  val formatter = TextFileRDD.formatter.asInstanceOf[JsonFormat[RDD]]
}

object TextFileRDD {
  val rddType = "textFile"

  val formatter = new RootJsonFormat[TextFileRDD] {
    override def write(obj: TextFileRDD): JsValue = JsObject(
      "path" -> obj.path.toJson,
      "type" -> TextFileRDD.rddType.toJson
    )

    override def read(json: JsValue): TextFileRDD = TextFileRDD(json.asJsObject.fields.get("path").get.asInstanceOf[JsString].value)
  }
}
