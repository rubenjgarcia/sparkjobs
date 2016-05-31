package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object SortByTransformations {
  case class SortByKeyTransformation(override val properties: Map[String, Any]) extends Transformation(properties) {
    override val `type`: String = "sortByKey"

    def apply(rdd: RDD[_]): RDD[_] = rdd.asInstanceOf[RDD[(String, Int)]].sortBy(_._1, properties.get("ascending").get.asInstanceOf[Boolean])
  }

  case class SortByValueTransformation(override val properties: Map[String, Any]) extends Transformation(properties) {
    override val `type`: String = "sortByValue"

    def apply(rdd: RDD[_]): RDD[_] = rdd.asInstanceOf[RDD[(String, Int)]].sortBy(_._2, properties.get("ascending").get.asInstanceOf[Boolean])
  }
}
