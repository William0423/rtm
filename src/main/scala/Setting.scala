import com.typesafe.config._

import scala.collection.JavaConversions._

case class TopicInfo(topic: String, parallel: Int, zookeeper: Option[String])

class ComponentSetting(config: Config) {
  val enabled = config.getBoolean("enabled")

  // it's parallelism of the Dstream, acutally compute parallelism.
//  val parallelism = config.getInt("parallel")

  // parallelism of reading.
  val topics = config.getConfigList("topics").map(c => {
    val zk = if (c.hasPath("zookeeper")) Some(c.getString("zookeeper")) else None
    new TopicInfo(c.getString("name"), c.getInt("parallel"), zk)
  }).toList
}