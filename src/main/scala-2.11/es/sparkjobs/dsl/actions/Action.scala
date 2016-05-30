package es.sparkjobs.dsl.actions

import org.apache.spark.rdd.RDD

trait Action[T] {
  def apply(rdd: RDD[_]) : T
}

case class CollectAction() extends Action[Array[_]] {
  override def apply(rdd: RDD[_]) = rdd.collect()
}