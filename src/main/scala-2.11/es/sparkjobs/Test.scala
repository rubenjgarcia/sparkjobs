package es.sparkjobs

import org.apache.spark._
import spray.json._
import SparkJobProtocol._
import es.sparkjobs.rdds.TextFileRDD
import es.sparkjobs.transformations._

import scala.io.Source

object Test extends App {

  override def main(args: Array[String]): Unit = {
    val path = getClass.getResource("/").getPath
    val source = Source.fromFile(path + "/../../../src/main/resources/job.json").getLines.mkString
    val job = source.parseJson.convertTo[SparkJob]

    val conf = new SparkConf()
      .setAppName(job.config.appName)
      .setMaster(job.config.master)

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(job.rdds.head.asInstanceOf[TextFileRDD].path)
    val sortTransformation = job.transformations(6).asInstanceOf[SortByKeyTransformation[String, Int]]
    val counts = textFile.flatMap(job.transformations(0).asInstanceOf[SplitFlatMapTransformation].unapply)
      .filter(job.transformations(1).asInstanceOf[FilterTransformation[String]].filter)
      .map(job.transformations(2).asInstanceOf[ReplaceAllMapTransformation].unapply)
      .map(job.transformations(3).asInstanceOf[LowerCaseMapTransformation].unapply)
      .map(job.transformations(4).asInstanceOf[TuplelizeMapTransformation[String, Int]].unapply)
      .reduceByKey(job.transformations(5).asInstanceOf[ReduceByKeySumTransformation[Int]].unapply)
      .sortBy(sortTransformation.unapply, sortTransformation.ascending)
    val words = counts.collect()
    words.foreach(println)
    println(words.length)
  }
}
