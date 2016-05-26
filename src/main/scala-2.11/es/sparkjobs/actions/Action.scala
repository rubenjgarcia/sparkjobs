package es.sparkjobs.actions

import spray.json._

//  val reduce = 100
//  val count = 102
//  val first = 103
//  val take = 104
//  val takeSample = 105
//  val takeOrdered = 106
//  val saveAsTextFile = 107
//  val saveAsSequenceFile = 108
//  val saveAsObjectFile = 109
//  val countByKey = 110
//  val foreach = 111

trait Action extends Serializable {
  val formatter: JsonFormat[Action]
}

object Action {
  case class ActionNotRegisteredException(msg: String) extends Exception(msg)

  val actions = scala.collection.mutable.HashMap.empty[String, JsonFormat[Action]]

  def register[T <: Action](actionType: String, formatter: JsonFormat[T]) : Unit = {
    actions += (actionType -> formatter.asInstanceOf[JsonFormat[Action]])
  }

  Action.register(CollectAction.actionType, CollectAction.formatter)

  implicit val formatter = new JsonFormat[Action] {
    override def write(obj: Action): JsValue = obj.formatter.write(obj)

    override def read(json: JsValue): Action = {
      val jsonActionType = json.asJsObject.fields.get("type").get.asInstanceOf[JsString].value
      val actionFormatter = actions.get(jsonActionType)
      if (actionFormatter.isEmpty) {
        throw new ActionNotRegisteredException(s"There is no formatter for action of type $jsonActionType")
      } else {
        actionFormatter.get.read(json)
      }
    }
  }
}
