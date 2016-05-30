package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object FilterTransformations {

  case class LengthFilterTransformation(length: Int) extends Transformation {
    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[CharSequence]].filter(t => t.length > length)
  }
}
