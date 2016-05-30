package es.sparkjobs.dsl.actions

import org.apache.spark.rdd.RDD

//  val reduce = 100
//  val takeSample = 105
//  val takeOrdered = 106
//  val saveAsSequenceFile = 108
//  val countByKey = 110
//  val foreach = 111

trait Action[T] {
  def apply(rdd: RDD[_]) : T
}

case class CollectAction() extends Action[Array[_]] {
  override def apply(rdd: RDD[_]) = rdd.collect()
}

case class CountAction() extends Action[Long] {
  override def apply(rdd: RDD[_]) = rdd.count()
}

case class FirstAction() extends Action[Any] {
  override def apply(rdd: RDD[_]) = rdd.first()
}

case class TakeAction(n: Int) extends Action[Array[_]] {
  override def apply(rdd: RDD[_]) = rdd.take(n)
}

case class SaveAsTextFileAction(path: String) extends Action[Unit] {
  override def apply(rdd: RDD[_]) = rdd.saveAsTextFile(path)
}

case class SaveAsObjectFileAction(path: String) extends Action[Unit] {
  override def apply(rdd: RDD[_]) = rdd.saveAsObjectFile(path)
}