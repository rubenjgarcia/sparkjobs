package es.sparkjobs.actions

import spray.json.DefaultJsonProtocol._
import spray.json._

case class CollectAction() extends Action {
  @transient val formatter = CollectAction.formatter.asInstanceOf[JsonFormat[Action]]
}

object CollectAction {
  val actionType = "collect"

  val formatter = new RootJsonFormat[CollectAction] {
    override def write(obj: CollectAction): JsValue = JsObject(
      "type" -> CollectAction.actionType.toJson
    )

    override def read(json: JsValue): CollectAction = CollectAction()
  }
}
