package es.sparkjobs.dsl.actions

import org.apache.spark.rdd.RDD
import scaldi.Module

//  val reduce = 100
//  val takeSample = 105
//  val takeOrdered = 106
//  val saveAsSequenceFile = 108
//  val countByKey = 110
//  val foreach = 111

abstract class Action[T](val properties: Map[String, String] = Map.empty) {
  val `type`: String
  def apply(rdd: RDD[_]) : T
}

case class CollectAction() extends Action[Array[_]] {
  override val `type`: String = "collect"
  override def apply(rdd: RDD[_]) = rdd.collect()
}

case class CountAction() extends Action[Long] {
  override val `type`: String = "count"
  override def apply(rdd: RDD[_]) = rdd.count()
}

case class FirstAction() extends Action[Any] {
  override val `type`: String = "first"
  override def apply(rdd: RDD[_]) = rdd.first()
}

case class TakeAction(override val properties: Map[String, String]) extends Action[Array[_]](properties) {
  override val `type`: String = "take"
  override def apply(rdd: RDD[_]) = rdd.take(properties.get("n").get.toInt)
}

case class SaveAsTextFileAction(override val properties: Map[String, String]) extends Action[Unit](properties) {
  override val `type`: String = "saveAsTextFileAction"
  override def apply(rdd: RDD[_]) = rdd.saveAsTextFile(properties.get("path").get)
}

case class SaveAsObjectFileAction(override val properties: Map[String, String]) extends Action[Unit](properties) {
  override val `type`: String = "saveAsObjectFileAction"
  override def apply(rdd: RDD[_]) = rdd.saveAsObjectFile(properties.get("path").get)
}