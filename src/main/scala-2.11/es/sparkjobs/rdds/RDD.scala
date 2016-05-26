package es.sparkjobs.rdds

import spray.json._

trait RDD {
  val formatter: JsonFormat[RDD]
}

object RDD {
  case class RDDNotRegisteredException(msg: String) extends Exception(msg)

  val rdds = scala.collection.mutable.HashMap.empty[String, JsonFormat[RDD]]

  def register[T <: RDD](rddType: String, formatter: JsonFormat[T]) : Unit = {
    rdds += (rddType -> formatter.asInstanceOf[JsonFormat[RDD]])
  }

  RDD.register(TextFileRDD.rddType, TextFileRDD.formatter)

  implicit val formatter = new JsonFormat[RDD] {
    override def write(obj: RDD): JsValue = obj.formatter.write(obj)

    override def read(json: JsValue): RDD = {
      val jsonRddType = json.asJsObject.fields.get("type").get.asInstanceOf[JsString].value
      val rddFormatter = rdds.get(jsonRddType)
      if (rddFormatter.isEmpty) {
        throw new RDDNotRegisteredException(s"There is no formatter for rdd of type $jsonRddType")
      } else {
        rddFormatter.get.read(json)
      }
    }
  }
}
