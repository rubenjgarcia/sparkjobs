package es.sparkjobs.dsl.rdds

import org.apache.spark.SparkContext

case class WholeTextFilesRDD(override val properties: Map[String, String]) extends RDD(properties) {
  override val `type`: String = "wholeTextFiles"

  override def toSparkRDD(implicit sc: SparkContext) = sc.wholeTextFiles(properties.get("path").get)
}
