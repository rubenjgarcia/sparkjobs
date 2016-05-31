package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object FilterTransformations {

  case class LengthFilterTransformation(override val properties: Map[String, Any]) extends Transformation(properties) {
    override val `type`: String = "lengthFilter"

    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[CharSequence]].filter(t => t.length > properties.get("length").asInstanceOf[Int])
  }
}
