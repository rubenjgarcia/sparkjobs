package es.sparkjobs.json

import es.sparkjobs.dsl.Config
import es.sparkjobs.dsl.SparkJob
import es.sparkjobs.dsl.SparkJob._
import es.sparkjobs.dsl.rdds.RDD
import es.sparkjobs.dsl.transformations._
import es.sparkjobs.dsl.actions._
import scaldi.Injectable
import spray.json._

object SparkJobJsonProtocol extends DefaultJsonProtocol {
  implicit val configFormat = jsonFormat2(Config.apply)

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case s: String => JsString(s)
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse
      case v: Vector[_] => JsArray(v.map(e => {
        write(read(e.asInstanceOf[JsValue]))
      }))
    }

    def read(value: JsValue) = value match {
      case JsNumber(n) => n.intValue()
      case JsString(s) => s
      case JsTrue => true
      case JsFalse => false
      case JsArray(arr) => arr
    }
  }

  implicit object SparkJobJsonFormat extends RootJsonFormat[SparkJob] with Injectable {
    override def write(job: SparkJob) = JsObject(
      "config" -> job.config.toJson,
      "rdds" -> JsArray(job.rdds.foldLeft(List[JsValue]())((list, rdd) => {
        list :+ JsObject(
          "type" -> rdd.`type`.toJson,
          "properties" -> rdd.properties.toJson,
          "actions" -> {
            JsArray(rdd.actions.foldLeft(List[JsValue]())((list, action) => {
              list :+ JsObject("type" -> action.`type`.toJson)
            }): _*)
          },
          "transformations" -> {
            JsArray(rdd.transformations.foldLeft(List[JsValue]())((list, transformation) => {
              val jso = if (transformation.properties.isEmpty) {
                JsObject("type" -> transformation.`type`.toJson)
              } else {
                JsObject(
                  "type" -> transformation.`type`.toJson,
                  "properties" -> transformation.properties.toJson
                )
              }
              list :+ jso
            }): _*)
          }
        )
      }): _*)
    )

    override def read(json: JsValue): SparkJob = {
      val fields: Map[String, JsValue] = json.asJsObject.fields

      val config: Config = configFormat.read(fields.get("config").get)

      val rddsJson: JsArray = fields.get("rdds").get.asInstanceOf[JsArray]
      val rdds = rddsJson.elements.map(value => {
        val rddType = value.asJsObject.fields.get("type").get.convertTo[String]
        val cl: Class[RDD] = inject[Class[_]](rddType).asInstanceOf[Class[RDD]]
        val constructor = cl.getDeclaredConstructors.head

        val properties = value.asJsObject.fields.get("properties").get.convertTo[Map[String, String]]
        val rdd = constructor.newInstance(properties).asInstanceOf[RDD]

        val actionsJson = value.asJsObject.fields.get("actions").get.asInstanceOf[JsArray]
        actionsJson.elements.foreach(value => {
          val actionType = value.asJsObject.fields.get("type").get.convertTo[String]
          val cl: Class[Action[_]] = inject[Class[_]](actionType).asInstanceOf[Class[Action[_]]]
          val constructor = cl.getDeclaredConstructors.head

          val properties: Map[String, String] = value.asJsObject.fields.getOrElse("properties", Map[String, String]()).asInstanceOf[Map[String, String]]
          val action = if (properties.isEmpty) constructor.newInstance() else constructor.newInstance(properties)
          rdd.action(action.asInstanceOf[Action[_]])
        })

        val transformationJson = value.asJsObject.fields.get("transformations").get.asInstanceOf[JsArray]
        transformationJson.elements.foreach(value => {
          val transformationType = value.asJsObject.fields.get("type").get.convertTo[String]
          val cl: Class[Transformation] = inject[Class[_]](transformationType).asInstanceOf[Class[Transformation]]
          val constructor = cl.getDeclaredConstructors.head

          val properties = value.asJsObject.fields.getOrElse("properties", JsObject()).asInstanceOf[JsObject]
          val transformation = if (properties.fields.isEmpty) constructor.newInstance() else constructor.newInstance(properties.convertTo[Map[String, Any]])
          rdd.addTransformation(transformation.asInstanceOf[Transformation])
        })

        rdd
      })

      SparkJob(config, rdds: _*)
    }
  }

}
