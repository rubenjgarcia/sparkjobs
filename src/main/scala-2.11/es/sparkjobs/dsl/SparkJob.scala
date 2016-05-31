package es.sparkjobs.dsl

import es.sparkjobs.dsl.actions._
import es.sparkjobs.dsl.rdds._
import es.sparkjobs.dsl.transformations.FlatMapTransformations._
import es.sparkjobs.dsl.transformations.FilterTransformations._
import es.sparkjobs.dsl.transformations.MapTransformations._
import es.sparkjobs.dsl.transformations.ReduceByKeyTransformations._
import es.sparkjobs.dsl.transformations.SortByTransformations._
import org.apache.spark._
import scaldi.Module

case class Config(appName: String, master: String)

case class SparkJob(config: Config, rdds: RDD*) {
  def execute(): Any = {
    val conf = new SparkConf()
      .setAppName(config.appName)
      .setMaster(config.master)

    implicit val sc = SparkContext.getOrCreate(conf)

    val rdd = rdds.head
    val rddT = rdd.transformations.foldLeft[org.apache.spark.rdd.RDD[_]](rdd.toSparkRDD)((rdd, transformation) => {
      transformation(rdd)
    })

    rdd.actions.head(rddT)
  }
}

object SparkJob {
  implicit val injector = new Module {
    bind[Class[_]] identifiedBy 'textFile to classOf[TextFileRDD]
    bind[Class[_]] identifiedBy 'wholeTextFiles to classOf[WholeTextFilesRDD]

    bind[Class[_]] identifiedBy 'collect to classOf[CollectAction]
    bind[Class[_]] identifiedBy 'count to classOf[CountAction]
    bind[Class[_]] identifiedBy 'first to classOf[FirstAction]
    bind[Class[_]] identifiedBy 'saveAsObjectFile to classOf[SaveAsObjectFileAction]
    bind[Class[_]] identifiedBy 'saveAsTextFile to classOf[SaveAsTextFileAction]
    bind[Class[_]] identifiedBy 'take to classOf[TakeAction]

    bind[Class[_]] identifiedBy 'lengthFilter to classOf[LengthFilterTransformation]
    bind[Class[_]] identifiedBy 'split to classOf[SplitFlatMapTransformation]
    bind[Class[_]] identifiedBy 'replaceAll to classOf[ReplaceAllMapTransformation]
    bind[Class[_]] identifiedBy 'lowerCase to classOf[LowerCaseMapTransformation]
    bind[Class[_]] identifiedBy 'upperCase to classOf[UpperCaseMapTransformation]
    bind[Class[_]] identifiedBy 'tuplelize to classOf[TuplelizeMapTransformation[_]]
    bind[Class[_]] identifiedBy 'reduceByKeySum to classOf[ReduceByKeySum]
    bind[Class[_]] identifiedBy 'sortByKey to classOf[SortByKeyTransformation]
    bind[Class[_]] identifiedBy 'sortByValue to classOf[SortByValueTransformation]
  }
}
