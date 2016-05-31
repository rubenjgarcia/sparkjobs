package es.sparkjobs.dsl

package object rdds {
  implicit def fromFile(path: String) = TextFileRDD(Map("path" -> path))
  implicit def fromFiles(path: String) = WholeTextFilesRDD(Map("path" -> path))
}
