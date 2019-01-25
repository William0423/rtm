
import com.typesafe.config._

object ConfTest {

    val config = ConfigFactory.load()

    val dbparams = new DBParams(
        config.getString("db.subprotocol"),
        config.getString("db.host"),
        config.getInt("db.port"),
        config.getString("db.user"),
        config.getString("db.passwd"),
        config.getString("db.database"))

    val kafkaParams = Map(
        "zookeeper.connect" -> config.getString("kafka.zookeeper"),
        "group.id" -> config.getString("kafka.group"),
        "zookeeper.connection.timeout.ms" -> "1000")

    val batchWindow = config.getInt("streaming.window")

    val rtbTopic = config.getString("kafka.rtb.topic")
    val rtbTopicParallel = config.getInt("kafka.rtb.parallel")

    val winTopic = config.getString("kafka.win.topic")
    val winTopicParallel = config.getInt("kafka.win.parallel")

    val trackingTopic = config.getString("kafka.tracking.topic")
    val trackingTopicParallel = config.getInt("kafka.tracking.parallel")

    val rtbEnabled = config.getBoolean("components.rtb");
    val winEnabled = config.getBoolean("components.win");
    val trackingEnabled = config.getBoolean("components.tracking");
    

    def main(args: Array[String]) {
        println(rtbEnabled)
    }
}
