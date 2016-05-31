package es.sparkjobs.test

import java.util.UUID

import es.sparkjobs.dsl.SparkJobDSL._
import es.sparkjobs.dsl.rdds._
import es.sparkjobs.dsl.transformations._
import es.sparkjobs.dsl.actions._

import org.scalatest._

class WordCountSpec extends UnitSpec {

  val source = getClass.getResource("/")
  val tmpdir = System.getProperty("java.io.tmpdir")

  val transformations = split(" ") andWith
      filter(3) andWith
      replaceAll(Array(",", ":", ";", "[?]", "¿", "!", "¡", "\"", "[(]", "[)]", "[.]"), "") andWith
      lowerCase andWith
      tuplelize(1) andWith
      reduceByKeySum andWith
      sortByValue(false)

  it should "get first word" in {
    val countWordRDD: RDD = fromFile(source + "quijote.txt") transformWith transformations

    val job = {
      config("Word Count", "local") rdd (countWordRDD action first)
    }

    val result = job.execute()
    result shouldBe("quijote", 362)
  }

  it should "get first 3 words" in {
    val countWordRDD: RDD = fromFile(source + "quijote.txt") transformWith transformations

    val job = {
      config("Word Count", "local") rdd (countWordRDD action take(3))
    }

    val result = job.execute()
    result shouldBe Array(("quijote", 362), ("como", 343), ("sancho", 299))
  }

  it should "count differents words" in {
    val countWordRDD: RDD = fromFile(source + "quijote.txt") transformWith transformations

    val job = {
      config("Word Count", "local") rdd (countWordRDD action count)
    }

    val result = job.execute()
    result shouldBe 7372
  }

  it should "collects words" in {
    val countWordRDD: RDD = fromFile(source + "quijote.txt") transformWith transformations

    val job = {
      config("Word Count", "local") rdd (countWordRDD action collect)
    }

    val result = job.execute().asInstanceOf[Array[(String, Int)]]
    result should have length 7372
    result(5) should be ("bien", 188)
  }

  it should "save as text file" in {
    val countWordRDD: RDD = fromFile(source + "quijote.txt") transformWith transformations

    val job = {
      config("Word Count", "local") rdd (countWordRDD action saveAsTextFile("file:///" + tmpdir + "/wordcount-txt-" + UUID.randomUUID()))
    }

    job.execute()
  }

  it should "save as object file" in {
    val countWordRDD: RDD = fromFile(source + "quijote.txt") transformWith transformations

    val job = {
      config("Word Count", "local") rdd (countWordRDD action saveAsObjectFile("file:///" + tmpdir + "/wordcount-obj-" + UUID.randomUUID()))
    }

    job.execute()
  }
}
