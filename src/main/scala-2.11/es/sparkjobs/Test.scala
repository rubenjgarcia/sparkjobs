package es.sparkjobs

import es.sparkjobs.dsl.SparkJobDSL._
import es.sparkjobs.dsl.rdds._
import es.sparkjobs.dsl.transformations._
import es.sparkjobs.dsl.actions._

object Test extends App {
  override def main(args: Array[String]): Unit = {
    val job = {
      config("Word Count", "local[8]") rdd {
        fromfile("/home/rjgarcia/Documents/Projects/SparkJobs/target/scala-2.11/classes/../../../src/main/resources/quijote.txt") transformWith {
          split(" ") andWith
            filter(3) andWith
            replaceAll(Array(",", ":", ";", "[?]", "¿", "!", "¡", "\"", "[(]", "[)]", "[.]"), "") andWith
            lowerCase andWith
            tuplelize(1) andWith
            reduceByKeySum andWith
            sortByValue(false)
        } action collect
      }
    }

    val lines = job.execute().asInstanceOf[Array[(String, Int)]]
    lines.foreach(println)
  }
}
