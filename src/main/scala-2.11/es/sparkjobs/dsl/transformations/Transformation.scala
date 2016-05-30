package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

//  val mapPartitions = 3
//  val mapPartitionsWithIndex = 4
//  val sample = 5
//  val union = 6
//  val intersection = 7
//  val distinct = 8
//  val groupByKey = 9
//  val aggregateByKey = 11
//  val join = 13
//  val cogroup = 14
//  val cartesian = 15
//  val pipe = 16
//  val coalesce = 17
//  val repartition = 18
//  val repartitionAndSortWithinPartitions = 19

abstract class Transformation {
  def apply(rdd: RDD[_]) : RDD[_]
}
