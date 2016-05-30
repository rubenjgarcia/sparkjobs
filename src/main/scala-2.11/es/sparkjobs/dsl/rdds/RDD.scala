package es.sparkjobs.dsl.rdds

import es.sparkjobs.dsl.transformations.Transformation
import es.sparkjobs.dsl.actions.Action
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

trait RDD {
  val transformations = ListBuffer[Transformation]()
  val actions = ListBuffer[Action[_]]()

  def addTransformation(t: Transformation*) = {
    transformations ++= t
  }

  def action(a: Action[_]) : RDD = {
    actions += a
    this
  }

  def toSparkRDD(implicit sc: SparkContext) : org.apache.spark.rdd.RDD[_]
}
