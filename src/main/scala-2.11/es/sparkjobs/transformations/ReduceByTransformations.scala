package es.sparkjobs.transformations

import spray.json._
import spray.json.DefaultJsonProtocol._

case class ReduceByKeySumTransformation[N]() extends Transformation {
  @transient val formatter = ReduceByKeySumTransformation.formatter.asInstanceOf[JsonFormat[Transformation]]
  def unapply(a: N, b: N): N = {
    val value = a match {
      case i: Int => a.asInstanceOf[Int] + b.asInstanceOf[Int]
    }
    value.asInstanceOf[N]
  }
}

object ReduceByKeySumTransformation {
  val transformationType = "reduceByKeySum"

  val formatter = new RootJsonFormat[ReduceByKeySumTransformation[Any]] {
    override def write(obj: ReduceByKeySumTransformation[Any]): JsValue = JsObject(
      "type" -> ReduceByKeySumTransformation.transformationType.toJson
    )

    override def read(json: JsValue): ReduceByKeySumTransformation[Any] = {
      ReduceByKeySumTransformation[Any]()
    }
  }
}

