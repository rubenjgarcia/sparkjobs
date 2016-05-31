package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object ReduceByKeyTransformations {

  case class ReduceByKeySum() extends Transformation {
    override val `type`: String = "reduceByKeySum"

    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[(Any, Int)]].reduceByKey((a, b) => a + b)
  }

}
