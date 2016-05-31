package es.sparkjobs.test

import scaldi.Injectable
import es.sparkjobs.dsl.SparkJob._
import es.sparkjobs.dsl.rdds._
import es.sparkjobs.dsl.actions._
import es.sparkjobs.dsl.transformations.Transformation
import es.sparkjobs.dsl.transformations.FlatMapTransformations._
import es.sparkjobs.dsl.transformations.FilterTransformations._
import es.sparkjobs.dsl.transformations.MapTransformations._
import es.sparkjobs.dsl.transformations.ReduceByKeyTransformations._
import es.sparkjobs.dsl.transformations.SortByTransformations._

class DISpec extends UnitSpec with Injectable {

  it should "inject a 'textFile' RDD" in {
    val cl: Class[RDD] = inject[Class[_]]('textFile).asInstanceOf[Class[RDD]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, String] = Map("path" -> "test")
    val rdd = constructor.newInstance(properties)
    rdd shouldBe TextFileRDD(properties)
  }

  it should "inject a 'wholeTextFiles' RDD" in {
    val cl: Class[RDD] = inject[Class[_]]('wholeTextFiles).asInstanceOf[Class[RDD]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, String] = Map("path" -> "test")
    val rdd = constructor.newInstance(properties)
    rdd shouldBe WholeTextFilesRDD(properties)
  }

  it should "inject a 'collect' Action" in {
    val cl: Class[Action[_]] = inject[Class[_]]('collect).asInstanceOf[Class[Action[_]]]
    val constructor = cl.getDeclaredConstructors.head
    val rdd = constructor.newInstance()
    rdd shouldBe CollectAction()
  }

  it should "inject a 'count' Action" in {
    val cl: Class[Action[_]] = inject[Class[_]]('count).asInstanceOf[Class[Action[_]]]
    val constructor = cl.getDeclaredConstructors.head
    val rdd = constructor.newInstance()
    rdd shouldBe CountAction()
  }

  it should "inject a 'first' Action" in {
    val cl: Class[Action[_]] = inject[Class[_]]('first).asInstanceOf[Class[Action[_]]]
    val constructor = cl.getDeclaredConstructors.head
    val rdd = constructor.newInstance()
    rdd shouldBe FirstAction()
  }

  it should "inject a 'saveAsObjectFile' Action" in {
    val cl: Class[Action[_]] = inject[Class[_]]('saveAsObjectFile).asInstanceOf[Class[Action[_]]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, String] = Map("path" -> "test")
    val rdd = constructor.newInstance(properties)
    rdd shouldBe SaveAsObjectFileAction(properties)
  }

  it should "inject a 'saveAsTextFile' Action" in {
    val cl: Class[Action[_]] = inject[Class[_]]('saveAsTextFile).asInstanceOf[Class[Action[_]]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, String] = Map("path" -> "test")
    val rdd = constructor.newInstance(properties)
    rdd shouldBe SaveAsTextFileAction(properties)
  }

  it should "inject a 'take' Action" in {
    val cl: Class[Action[_]] = inject[Class[_]]('take).asInstanceOf[Class[Action[_]]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, String] = Map("n" -> "2")
    val rdd = constructor.newInstance(properties)
    rdd shouldBe TakeAction(properties)
  }

  it should "inject a 'lengthFilter' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('lengthFilter).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, String] = Map("length" -> "2")
    val rdd = constructor.newInstance(properties)
    rdd shouldBe LengthFilterTransformation(properties)
  }

  it should "inject a 'split' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('split).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, String] = Map("regex" -> " ")
    val rdd = constructor.newInstance(properties)
    rdd shouldBe SplitFlatMapTransformation(properties)
  }

  it should "inject a 'replaceAll' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('replaceAll).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, String] = Map("regex" -> " ", "replacement" -> "-")
    val rdd = constructor.newInstance(properties)
    rdd shouldBe ReplaceAllMapTransformation(properties)
  }

  it should "inject a 'lowerCase' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('lowerCase).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val rdd = constructor.newInstance()
    rdd shouldBe LowerCaseMapTransformation()
  }

  it should "inject a 'upperCase' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('upperCase).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val rdd = constructor.newInstance()
    rdd shouldBe UpperCaseMapTransformation()
  }

  it should "inject a 'tuplelize' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('tuplelize).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, Int] = Map("value" -> 1)
    val rdd = constructor.newInstance(properties)
    rdd shouldBe TuplelizeMapTransformation(properties)
  }

  it should "inject a 'reduceByKeySum' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('reduceByKeySum).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val rdd = constructor.newInstance()
    rdd shouldBe ReduceByKeySum()
  }

  it should "inject a 'sortByKey' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('sortByKey).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, Boolean] = Map("ascending" -> true)
    val rdd = constructor.newInstance(properties)
    rdd shouldBe SortByKeyTransformation(properties)
  }

  it should "inject a 'sortByValue' Transformation" in {
    val cl: Class[Transformation] = inject[Class[_]]('sortByValue).asInstanceOf[Class[Transformation]]
    val constructor = cl.getDeclaredConstructors.head
    val properties: Map[String, Boolean] = Map("ascending" -> true)
    val rdd = constructor.newInstance(properties)
    rdd shouldBe SortByValueTransformation(properties)
  }
}
