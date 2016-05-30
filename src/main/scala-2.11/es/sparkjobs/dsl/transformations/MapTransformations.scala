package es.sparkjobs.dsl.transformations

import org.apache.spark.rdd.RDD

object MapTransformations {

  case class ReplaceAllMapTransformation(regex: Array[String],
                                         replacement: String,
                                         regexWithReplacement: (String, String)*
                                        ) extends Transformation {
    override def apply(rdd: RDD[_]) = {
      if (regexWithReplacement.isEmpty) {
        rdd.asInstanceOf[RDD[String]].map(s => {
          regex.foldLeft(s)((value, search) => {
            value.replaceAll(search, replacement)
          })
        })
      } else {
        rdd.asInstanceOf[RDD[String]].map(s => regexWithReplacement.foldLeft(s)((s, r) => s.replaceAll(r._1, r._2)))
      }
    }
  }

  case class LowerCaseMapTransformation() extends Transformation {
    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[String]].map(s => s.toLowerCase)
  }

  case class UpperCaseMapTransformation() extends Transformation {
    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[String]].map(s => s.toUpperCase)
  }

  case class TuplelizeMapTransformation[V](value: V) extends Transformation {
    override def apply(rdd: RDD[_]) = rdd.asInstanceOf[RDD[String]].map(s => (s, value))
  }

}
