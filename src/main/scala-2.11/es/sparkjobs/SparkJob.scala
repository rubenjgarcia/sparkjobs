package es.sparkjobs

import es.sparkjobs.actions.Action
import es.sparkjobs.rdds.RDD
import es.sparkjobs.transformations.Transformation
import spray.json._

case class Config(appName: String, master: String)

case class SparkJob(config: Config, rdds: List[RDD], transformations: List[Transformation], actions: List[Action])

object SparkJobProtocol extends DefaultJsonProtocol {
  implicit val configFormat = jsonFormat2(Config.apply)

  implicit object SparkJobJsonFormat extends RootJsonFormat[SparkJob] {
    override def write(job: SparkJob) = JsObject(
      "config" -> job.config.toJson,
      "rdds" -> JsArray(job.rdds.foldLeft(List[JsValue]())((list, rdd) => list :+ rdd.toJson): _*),
      "transformations" -> JsArray(job.transformations.foldLeft(List[JsValue]())((list, transformation) => list :+ transformation.toJson): _*),
      "actions" -> JsArray(job.actions.foldLeft(List[JsValue]())((list, action) => list :+ action.toJson): _*)
    )

    override def read(json: JsValue): SparkJob = {
      val fields: Map[String, JsValue] = json.asJsObject.fields

      val config: Config = configFormat.read(fields.get("config").get)

      val rddsJson: JsArray = fields.get("rdds").get.asInstanceOf[JsArray]
      val rdds = rddsJson.elements.foldLeft(List[RDD]())((list, value) => {
        list :+ value.convertTo[RDD]
      })

      val transformationsJson: JsArray = fields.get("transformations").get.asInstanceOf[JsArray]
      val transformations = transformationsJson.elements.foldLeft(List[Transformation]())((list, value) => {
        list :+ value.convertTo[Transformation]
      })

      val actionsJson: JsArray = fields.get("actions").get.asInstanceOf[JsArray]
      val actions = actionsJson.elements.foldLeft(List[Action]())((list, value) => {
        list :+ value.convertTo[Action]
      })

      SparkJob(config, rdds, transformations, actions)
    }
  }
}
