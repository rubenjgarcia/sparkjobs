package es.sparkjobs.transformations

import spray.json.DefaultJsonProtocol._
import spray.json._

abstract class SortByTransformation[A, B] extends Transformation {
  val ascending: Boolean = true
  def unapply(t: A) : B
}

case class SortByKeyTransformation[K, V](override val ascending: Boolean = true) extends SortByTransformation[(K,V), K] {
  @transient val formatter = SortByKeyTransformation.formatter.asInstanceOf[JsonFormat[Transformation]]

  override def unapply(t: (K, V)) : K = {
    t._1
  }
}

object SortByKeyTransformation {
  val transformationType = "sortByKey"

  val formatter = new RootJsonFormat[SortByKeyTransformation[Any, Any]] {
    override def write(obj: SortByKeyTransformation[Any, Any]): JsValue = JsObject(
      "type" -> SortByKeyTransformation.transformationType.toJson,
      "ascending" -> obj.ascending.toJson
    )

    override def read(json: JsValue): SortByKeyTransformation[Any, Any] = SortByKeyTransformation(json.asJsObject.fields.getOrElse("ascending", JsBoolean.apply(true)).asInstanceOf[JsBoolean].value)
  }
}

case class SortByValueTransformation[K, V](override val ascending: Boolean = true) extends SortByTransformation[(K,V), V] {
  @transient val formatter = SortByValueTransformation.formatter.asInstanceOf[JsonFormat[Transformation]]

  override def unapply(t: (K, V)) : V = {
    t._2
  }
}

object SortByValueTransformation {
  val transformationType = "sortByValue"

  val formatter = new RootJsonFormat[SortByValueTransformation[Any, Any]] {
    override def write(obj: SortByValueTransformation[Any, Any]): JsValue = JsObject(
      "type" -> SortByValueTransformation.transformationType.toJson,
      "ascending" -> obj.ascending.toJson
    )

    override def read(json: JsValue): SortByValueTransformation[Any, Any] = SortByValueTransformation(json.asJsObject.fields.getOrElse("ascending", JsBoolean.apply(true)).asInstanceOf[JsBoolean].value)
  }
}
