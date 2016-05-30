package es.sparkjobs.dsl

import es.sparkjobs.dsl.transformations.FlatMapTransformations._
import es.sparkjobs.dsl.transformations.FilterTransformations._
import es.sparkjobs.dsl.transformations.MapTransformations._
import es.sparkjobs.dsl.transformations.ReduceByKeyTransformations._
import es.sparkjobs.dsl.transformations.SortByTransformations._

package object transformations {
  implicit def split(regex: String) = SplitFlatMapTransformation(regex)
  implicit def filter(length: Int) = LengthFilterTransformation(length)
  implicit def replaceAll(regexWithReplacement: (String, String)*) = ReplaceAllMapTransformation(Array(), "", regexWithReplacement: _*)
  implicit def replaceAll(regex: Array[String], replace: String) = ReplaceAllMapTransformation(regex, replace)
  implicit def lowerCase() = LowerCaseMapTransformation()
  implicit def upperCase() = UpperCaseMapTransformation()
  implicit def tuplelize[V](v: V) = TuplelizeMapTransformation(v)
  implicit def reduceByKeySum = ReduceByKeySum()
  implicit def sortByKey(ascending: Boolean = true) = SortByKeyTransformation(ascending)
  implicit def sortByValue(ascending: Boolean = true) = SortByValueTransformation(ascending)
}
