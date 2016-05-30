package es.sparkjobs.dsl

package object actions {
  implicit def collect = CollectAction()
  implicit def count = CountAction()
  implicit def first = FirstAction()
  implicit def take(n: Int) = TakeAction(n)
  implicit def saveAsTextFile(path: String) = SaveAsTextFileAction(path)
  implicit def saveAsObjectFile(path: String) = SaveAsObjectFileAction(path)
}
