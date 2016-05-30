package es.sparkjobs.test

import es.sparkjobs.dsl.SparkJobDSL._
import es.sparkjobs.dsl.rdds._
import es.sparkjobs.dsl.transformations._
import es.sparkjobs.dsl.actions._
import org.scalatest._

class WordCountSpec extends UnitSpec {

  "It" should "count words" in {
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
        } action first
      }
    }

    val result = job.execute()
    result shouldBe ("quijote", 362)
  }
}
