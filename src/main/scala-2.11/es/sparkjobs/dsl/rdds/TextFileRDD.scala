package es.sparkjobs.dsl.rdds

import org.apache.spark.SparkContext

case class TextFileRDD(override val properties: Map[String, String]) extends RDD(properties) {
  override val `type`: String = "textFile"

  override def toSparkRDD(implicit sc: SparkContext) = sc.textFile(properties.get("path").get)
}
