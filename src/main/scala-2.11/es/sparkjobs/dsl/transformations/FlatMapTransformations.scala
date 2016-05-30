package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object FlatMapTransformations {

  case class SplitFlatMapTransformation(regex: String) extends Transformation {
    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[String]].flatMap(e => e.split(regex))
  }

}