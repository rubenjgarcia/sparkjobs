package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object FlatMapTransformations {

  case class SplitFlatMapTransformation(override val properties: Map[String, Any]) extends Transformation(properties) {
    override val `type`: String = "split"

    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[String]].flatMap(e => e.split(properties.get("regex").asInstanceOf[String]))
  }

}