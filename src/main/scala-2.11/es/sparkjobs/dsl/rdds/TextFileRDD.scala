package es.sparkjobs.dsl.rdds

import org.apache.spark.SparkContext

case class TextFileRDD(path: String) extends RDD {
  override def toSparkRDD(implicit sc: SparkContext) = sc.textFile(path)
}
