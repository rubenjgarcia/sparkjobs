package es.sparkjobs.dsl

package object actions {
  implicit def collect = CollectAction()
  implicit def count = CountAction()
  implicit def first = FirstAction()
  implicit def take(n: Int) = TakeAction(Map("n" -> n.toString))
  implicit def saveAsTextFile(path: String) = SaveAsTextFileAction(Map("path" -> path))
  implicit def saveAsObjectFile(path: String) = SaveAsObjectFileAction(Map("path" -> path))
}
