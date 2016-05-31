package es.sparkjobs.dsl

import es.sparkjobs.dsl.transformations.FlatMapTransformations._
import es.sparkjobs.dsl.transformations.FilterTransformations._
import es.sparkjobs.dsl.transformations.MapTransformations._
import es.sparkjobs.dsl.transformations.ReduceByKeyTransformations._
import es.sparkjobs.dsl.transformations.SortByTransformations._

package object transformations {
  implicit def split(regex: String) = SplitFlatMapTransformation(Map("regex" -> regex))
  implicit def filter(length: Int) = LengthFilterTransformation(Map("length" -> length))
  implicit def replaceAll(regexWithReplacement: (String, String)*) = ReplaceAllMapTransformation(Map("regexWithReplacement" -> regexWithReplacement))
  implicit def replaceAll(regex: Array[String], replace: String) = ReplaceAllMapTransformation(Map("regex" -> regex, "replace" -> replace))
  implicit def lowerCase() = LowerCaseMapTransformation()
  implicit def upperCase() = UpperCaseMapTransformation()
  implicit def tuplelize[V](v: V) = TuplelizeMapTransformation(Map("value" -> v))
  implicit def reduceByKeySum = ReduceByKeySum()
  implicit def sortByKey(ascending: Boolean = true) = SortByKeyTransformation(Map("ascending" -> ascending.toString))
  implicit def sortByValue(ascending: Boolean = true) = SortByValueTransformation(Map("ascending" -> ascending.toString))
}
