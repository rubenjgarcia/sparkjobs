package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object MapTransformations {

  case class ReplaceAllMapTransformation(override val properties: Map[String, Any]) extends Transformation(properties) {
    override val `type`: String = "replaceAll"

    override def apply(rdd: RDD[_]) = {
      val regex = properties.getOrElse("regex", Array[String]()).asInstanceOf[Array[String]]
      val replacement = properties.getOrElse("replacement", "").asInstanceOf[String]
      val regexWithReplacement = properties.getOrElse("regexWithReplacement", Array[(String, String)]()).asInstanceOf[Array[(String, String)]]
      if (regexWithReplacement.isEmpty) {
        rdd.asInstanceOf[RDD[String]].map(s => {
          regex.foldLeft(s)((value, search) => {
            value.replaceAll(search, replacement)
          })
        })
      } else {
        rdd.asInstanceOf[RDD[String]].map(s => regexWithReplacement.foldLeft(s)((s, r) => s.replaceAll(r._1, r._2)))
      }
    }
  }

  case class LowerCaseMapTransformation() extends Transformation {
    override val `type`: String = "lowerCase"

    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[String]].map(s => s.toLowerCase)
  }

  case class UpperCaseMapTransformation() extends Transformation {
    override val `type`: String = "upperCase"

    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[String]].map(s => s.toUpperCase)
  }

  case class TuplelizeMapTransformation[V](override val properties: Map[String, Any]) extends Transformation(properties) {
    override val `type`: String = "tuplelize"

    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[String]].map(s => (s, properties.get("value").get.asInstanceOf[V]))
  }

}
