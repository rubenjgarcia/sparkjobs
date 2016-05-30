package es.sparkjobs.dsl

import es.sparkjobs.dsl.rdds.RDD
import es.sparkjobs.dsl.transformations.Transformation
import es.sparkjobs.dsl.actions.Action

object SparkJobDSL {
  implicit def config(appName: String, master: String) = Config(appName, master)

  implicit def ConfigToSparkJob(config: Config) = SparkJob(config)
  implicit def ConfigRDDToSparkJob(configRDD: ConfigRDD) = SparkJob(configRDD._1, configRDD._2)

  type ConfigRDD = (Config, RDD)

  implicit def config2RDDHelper(config: Config) = new RDDHelper(config)

  class RDDHelper(config: Config) {
    def rdd(rdd: RDD) : ConfigRDD = (config, rdd)
  }

  type RDDTransformation = (RDD, List[Transformation])

  implicit def rdd2TransformationHelper(rdd: RDD) = new TransformationHelper(rdd, List[Transformation]())

  class TransformationHelper(rdd: RDD, transformations: List[Transformation]) {
    def transformWith(transformation: Transformation) : RDDTransformation = {
      (rdd, transformation :: transformations)
    }

    def transformWith(transformations: List[Transformation]) : RDDTransformation = {
      (rdd, transformations)
    }
  }

  implicit def rddTransformation2RDD(r: RDDTransformation) : RDD = {
    r._2.foreach(t => r._1.addTransformation(t))
    r._1
  }

  implicit def transformation2TransformationHelper(transformation: Transformation) = new TransformationsHelper(transformation)

  class TransformationsHelper(transformation1: Transformation) {
    def andWith(transformation2: Transformation) : List[Transformation] = List[Transformation](transformation1, transformation2)
  }

  implicit def transformation2TransformationListHelper(transformations: List[Transformation]) = new TransformationListHelper(transformations)

  class TransformationListHelper(transformations: List[Transformation]) {
    def andWith(transformation: Transformation) : List[Transformation] = transformations :+ transformation
  }
}