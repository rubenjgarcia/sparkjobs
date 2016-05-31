package es.sparkjobs.test

import es.sparkjobs.dsl.SparkJob
import spray.json._
import es.sparkjobs.json.SparkJobJsonProtocol._
import scala.io.Source

import org.scalatest._

class JSONSpec extends UnitSpec {

  val source = getClass.getResource("/")

  it should "parse a valid JSON" in {
    val json = Source.fromURL(source + "job.json").mkString.parseJson
    val job = json.convertTo[SparkJob]
    job.toJson shouldBe json
  }
}
