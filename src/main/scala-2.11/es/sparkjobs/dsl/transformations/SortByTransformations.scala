package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object SortByTransformations {
  case class SortByKeyTransformation(ascending: Boolean = true) extends Transformation {
    def apply(rdd: RDD[_]): RDD[_] = rdd.asInstanceOf[RDD[(String, Int)]].sortBy(_._1, ascending)
  }

  case class SortByValueTransformation(ascending: Boolean = true) extends Transformation {
    def apply(rdd: RDD[_]): RDD[_] = rdd.asInstanceOf[RDD[(String, Int)]].sortBy(_._2, ascending)
  }
}
