package es.sparkjobs.dsl

package object rdds {
  implicit def fromfile(path: String) = TextFileRDD(path)
}
