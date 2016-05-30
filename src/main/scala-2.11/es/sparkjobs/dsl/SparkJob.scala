package es.sparkjobs.dsl

import org.apache.spark._
import es.sparkjobs.dsl.rdds.RDD

case class Config(appName: String, master: String)

case class SparkJob(config: Config, rdds: RDD*) {
  def execute(): Any = {
    val conf = new SparkConf()
      .setAppName(config.appName)
      .setMaster(config.master)

    implicit val sc = new SparkContext(conf)

    val rdd = rdds.head
    val rddT = rdd.transformations.foldLeft[org.apache.spark.rdd.RDD[_]](rdd.toSparkRDD)((rdd, transformation) => {
      println("apply" + transformation)
      transformation(rdd)
    })

    rdd.actions.head(rddT)
  }
}
