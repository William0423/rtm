
import java.io.File

import scala.collection.mutable.{ArrayBuffer, Set}
import scala.io.Source.fromInputStream
import java.net._
import java.util.Properties

import scala.util.{Random, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.broadcast.{Broadcast, TorrentBroadcastFactory}
import com.typesafe.config._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.consumer
import org.apache.spark._
import akka.actor.Actor
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import utils.ZkWork
//import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteArrayDeserializer


case class EXE(name: String, dbp: DBParams)

case class SQLCMD(name: String, sql: String)

class SqlCmdQueueActor extends Actor with Logging {

  val tempSql = scala.collection.mutable.Map[String, scala.collection.mutable.ArrayBuffer[String]]()

  override def receive: Receive = {
    case SQLCMD(name, sql) =>
      if (tempSql.contains(name)) {
        tempSql.get(name).get += sql
      } else {
        tempSql.put(name, scala.collection.mutable.ArrayBuffer(sql))
      }

    case EXE(name, dbparams: DBParams) =>
      val dbconn = DBConnectionFactory.getConnection(dbparams)

      tempSql.getOrElse(name, List[String]()).foreach(dbconn.addBatch(_))
      try {
        dbconn.executeBatch()
      } catch {
        case e: java.sql.SQLException =>
          logError(e.getMessage)
      }
      tempSql.remove(name)
      dbconn.buffer.clear()
      DBConnectionFactory.returnConnection(dbconn)
    case _ =>
  }
}


class SqlCmdQueueActor1 extends Actor with Logging {

  val tempSql = scala.collection.mutable.Map[String, scala.collection.mutable.ArrayBuffer[String]]()

  override def receive: Receive = {
    case SQLCMD(name, sql) =>
      if (tempSql.contains(name)) {
        tempSql.get(name).get += sql
      } else {
        tempSql.put(name, scala.collection.mutable.ArrayBuffer(sql))
      }

    case EXE(name, dbparams: DBParams) =>
      println("SqlCmdQueueActor1 receive EXE ")
    case _ =>
  }
}

//------------------ end


object ReduceFunc {
  def sumIntInt(a: (Int, Int), b: (Int, Int)): (Int, Int) = {
    (a._1 + b._1, a._2 + b._2)
  }

  def sumIntLong(a: (Int, Long), b: (Int, Long)): (Int, Long) = {
    (a._1 + b._1, a._2 + b._2)
  }
}

object RTMStreamingAnalyze {
  //------------------------ akka asycn for exe sql-----------
  import akka.actor.ActorSystem
  import akka.util.Timeout

  import scala.concurrent.duration._

  implicit val timeout: Timeout = 5.seconds
  implicit val system = ActorSystem("RTMRoot")

  val cmdActor = system.actorOf(akka.actor.Props[SqlCmdQueueActor],
    name = "SqlCmdQueueActor")

  //------------------------ akka asycn for exe sql-----------

  //val parseFile = ConfigFactory.parseFile(new File("application.conf"))

  //val config = ConfigFactory.load(parseFile)
  val config = ConfigFactory.load()

  val dbparams = new DBParams(
    config.getString("db.subprotocol"),
    config.getString("db.host"),
    config.getInt("db.port"),
    config.getString("db.user"),
    config.getString("db.passwd"),
    config.getString("db.database")
  )

  //
  val initZids = scala.collection.mutable.Set[String]()

  //val globalZookeeper = config.getString("kafka.zookeeper")

  // currently not consider to change.
  val BATCH_WINDOW = 60
  val batchInterval = config.getInt("spark.streaming.batchInterval")
  //val sparkMaster = config.getString("spark.master")
  //val sparkExecutorMem = config.getString("spark.executor-memory")
  //val sparkExecutorCores = config.getString("spark.total-executor-cores")

  /*
  val zookeeperTimeout = config.getString("kafka.zookeeper_timeout")
  val zookeeperSessionTimeOut = config.getString("kafka.zookeeper_session_timeout")
  val zookeeperRebalanceBackoff = config.getString("kafka.zookeeper_rebalance_backoff")
  val zookeeperRebalanceMax = config.getString("kafka.zookeeper_rebalance_max")
  val zookeeperSyncTime = config.getString("kafka.zookeeper_sync_time")

  val checkPointDir = config.getString("spark.checkpointdir")
  val memoryFraction = config.getString("spark.memoryFraction")
  val blockQueueSize = config.getString("spark.blockQueueSize")



  val rtbBatchTasks = config.getInt("components.rtb.batchTasks")
  val ttlTime = config.getString("spark.ttlTime")
  val concurrentJobs = config.getString("spark.concurrentJobs")


   */
  val checkPointDir =  "/tmp/spark_streaming/check_point"
  val APP_NAME = config.getString("spark.appname")

  val rtbWindowTasks = config.getInt("components.rtb.windowTasks")
  // zampct
  val CT_API_HOST = config.getString("zampct.apiHost")
  val CT_ZIDS_API = config.getString("zampct.zids")
  val CT_RTB_PUSH_API = config.getString("zampct.pushRtb")
  val CT_CM_PUSH_API = config.getString("zampct.pushCm")
  val CT_SEG_PUSH_API = config.getString("zampct.pushSeg")
  val CT_RENDER_PUSH_API = config.getString("zampct.pushRender")
  val CT_TIMEOUT = config.getInt("zampct.timeout")

  val CHANCE = config.getString("kafka.chance").toFloat

  // debug param



  def createDirectStreams(topicInfo: TopicInfo, ssc: StreamingContext) = {


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.22.57.94:9092,172.22.57.95:9092,172.22.57.96:9092,172.22.57.97:9092,172.22.57.98:9092,172.22.56.41:9092,172.22.56.42:9092,172.22.56.43:9092,172.22.57.116:9092,172.22.57.117:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> s"streaming_rtm",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "max.poll.records" -> "100000"
    )

    var newOffset = Map[TopicPartition, Long]()


    var arr1 = ArrayBuffer[String]();
    arr1 += "--broker-list=172.22.57.94:9092"
    arr1 += s"-topic=${topicInfo.topic}"
    arr1 += "--time=-1"

    var arr2 = ArrayBuffer[String]();
    arr2 += "--broker-list=172.22.57.94:9092"
    arr2 += s"-topic=${topicInfo.topic}"
    arr2 += "--time=-2"

    val startOffset = utils.GetOffsetShellWrap.GetOffsetShell(arr2.toArray)
    val untilOffset = utils.GetOffsetShellWrap.GetOffsetShell(arr1.toArray)

    for (tp <- untilOffset.keys) {

      if (ZkWork.znodeIsExists(s"consumers/rtm/new/${tp.topic()}_${tp.partition()}")) {

        println(s"${tp.topic()}_${tp.partition()} reset zk offset to  until offset")
        ZkWork.znodeDataSet(s"consumers/rtm/new/${tp.topic()}_${tp.partition()}", s"${untilOffset.get(tp).get}")
        val res = ZkWork.znodeDataGet(s"consumers/rtm/new/${tp.topic()}_${tp.partition()}")
        newOffset += new TopicPartition(tp.topic(), tp.partition()) -> res.toLong

        /*
        val res = ZkWork.znodeDataGet(s"consumers/rtm/new/${tp.topic()}_${tp.partition()}")
        if (res != null && res.toLong > startOffset.get(tp).get && res.toLong < untilOffset.get(tp).get) {
          newOffset += new TopicPartition(tp.topic(), tp.partition()) -> res.toLong
          println(s"${tp.topic()}_${tp.partition()} use  zk offset :" + res.toLong)
        } else {

          println(s"${tp.topic()}_${tp.partition()} reset zk offset to  until offset")
          ZkWork.znodeDataSet(s"consumers/rtm/new/${tp.topic()}_${tp.partition()}", s"${untilOffset.get(tp).get}")
          val res = ZkWork.znodeDataGet(s"consumers/rtm/new/${tp.topic()}_${tp.partition()}")
          newOffset += new TopicPartition(tp.topic(), tp.partition()) -> res.toLong
        }
        */

      } else {

        ZkWork.znodeCreate(s"consumers/rtm/new/${tp.topic()}_${tp.partition()}", s"${untilOffset.get(tp).get}")
        val res = ZkWork.znodeDataGet(s"consumers/rtm/new/${tp.topic()}_${tp.partition()}")
        newOffset += new TopicPartition(tp.topic(), tp.partition()) -> res.toLong

      }
    }


    println(" ##### start offset : ")

    for (tp <- startOffset.keys) {
      println(tp.toString() + " -> " + startOffset.get(tp).get)
    }
    println(" ##### end offset : ")
    for (tp <- untilOffset.keys) {
      println(tp.toString() + " -> " + untilOffset.get(tp).get)
    }

    println(" ##### zk offset : ")
    for (tp <- newOffset.keys) {
      println(tp.toString() + " -> " + newOffset.get(tp).get)
    }

    val streams = KafkaUtils.createDirectStream[String, Array[Byte]](ssc, PreferConsistent, Assign[String, Array[Byte]](newOffset.keys.toList, kafkaParams, newOffset))


    streams

  }




  def createUnifiedStream(topics: List[TopicInfo], ssc: StreamingContext) = {
    val streams = topics.map(createDirectStreams(_, ssc))
    if (streams.size > 1) {
      ssc.union(streams) //.repartition(sparkRepartition)
    } else {
      println("stream size is 1")
      streams(0)
    }

  }


  /**
    * Save raw sql queries on each worker now.
    */
  def saveAsTextFile(path: String, content: Traversable[String]) = try {
    val bwr = new java.io.BufferedWriter(new java.io.FileWriter(path))
    content.foreach { s =>
      bwr.write(s)
      bwr.newLine()
    }
    bwr.close
  } catch {
    case _: Throwable =>

    /**
      * Sorry to hear that.
      */
  }

  def dbgWithTimestamp[T](results: DStream[T], decoder: T => Item) = {
    results.foreachRDD((rdd, time) => {
      val receiveTime = SQLBuilder.toSQLTimestamp(
        time.floor(Minutes(1)).milliseconds)


      rdd.foreachPartition { partitionOfRecords =>
        val ptime = System.currentTimeMillis()
        partitionOfRecords.foreach { record =>
          val item = decoder(record)
          val sql = SQLBuilder.build(item, receiveTime)
          if (sql != "") {
            println("record :" + sql)
          }
        }
      }

    })
  }

  def saveToDBWithTimestamp[T](results: DStream[T], decoder: T => Item)(implicit component: String) = {
    results.foreachRDD((rdd, time) => {
      val receiveTime = SQLBuilder.toSQLTimestamp(
        time.floor(Minutes(1)).milliseconds)


      rdd.foreachPartition { partitionOfRecords =>
        val ptime = System.currentTimeMillis()
        partitionOfRecords.foreach { record =>
          val item = decoder(record)
          val sql = SQLBuilder.build(item, receiveTime)
          if (sql != "") cmdActor ! SQLCMD(ptime.toString, sql)
        }
        cmdActor ! EXE(ptime.toString, dbparams)
      }

    })


  }

  def saveRTBDebugToDB(results: DStream[(String, Array[(String, String, String)])]) = {
    results.foreachRDD((rdd, time) => {
      val receiveTime = SQLBuilder.toSQLTimestamp(
        time.floor(Minutes(1)).milliseconds)


      rdd.foreachPartition { partitionOfRecords =>
        val ptime = System.currentTimeMillis()
        partitionOfRecords.foreach { record =>
          val zid = record._1
          for ((bid_data, res_data, mega_data) <- record._2) {
            val sql = SQLBuilder.t_rtb_debug_insert.format(receiveTime, zid, bid_data, res_data, mega_data)
            cmdActor ! SQLCMD(ptime.toString, sql)
          }
        }
        cmdActor ! EXE(ptime.toString, dbparams)
      }
    })
  }

  val RTBSetting = new ComponentSetting(config.getConfig("components.rtb"))
  val WinSetting = new ComponentSetting(config.getConfig("components.win"))
  val MWinSetting = new ComponentSetting(config.getConfig("components.mwin"))
  val ImpTrackingSetting = new ComponentSetting(config.getConfig("components.tracking_imp"))
  val ClkTrackingSetting = new ComponentSetting(config.getConfig("components.tracking_clk"))
  val ImpConvSetting = new ComponentSetting(config.getConfig("components.imp_conv"))
  val ImpYdConvSetting = new ComponentSetting(config.getConfig("components.imp_ydconv"))
  val DaspSetting = new ComponentSetting(config.getConfig("components.dasp"))
  val RTBDebugSetting = new ComponentSetting(config.getConfig("components.rtbdebug"))
  val UMASetting = new ComponentSetting(config.getConfig("components.uma"))
  val DaspMobSetting = new ComponentSetting(config.getConfig("components.daspmob"))
  val DaspSdkSetting = new ComponentSetting(config.getConfig("components.daspsdk"))

  // zampct
  val SegmentSettings = new ComponentSetting(config.getConfig("components.segment"))
  val CMSettings = new ComponentSetting(config.getConfig("components.idmap"))
  val RenderSettings = new ComponentSetting(config.getConfig("components.render"))


  import Parsers._
  import ReduceFunc._


  def computeRTB(ssc: StreamingContext, bc: Broadcast[Set[String]], component: String = "RTB") {

    val stream: DStream[ConsumerRecord[String, Array[Byte]]] = createDirectStreams(new TopicInfo("rtb_rtm", 1, null), ssc)

    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeRTBLog(r.value(), bc))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW), rtbWindowTasks)
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = RTBDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)

            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }
      })


  }


  //  def computeRTBDebug(implicit ssc: StreamingContext, component: String="RTBDebug") {
  //    if (!RTBDebugSetting.enabled) return
  //    val stream: DStream[(String, Array[Byte])] = createUnifiedStream(RTBDebugSetting.topics)
  //    val values: DStream[(String, Array[(String, String, String)])] = stream.map(r => analyzeRTBDebugLog(r._2))
  //
  //
  //    val combineValues = values.reduceByKey(_ ++ _)
  //
  //    combineValues.foreachRDD(_.count)
  //
  //    val accumValues = combineValues.reduceByKeyAndWindow(
  //      (a: Array[(String, String, String)], b: Array[(String, String, String)]) => a++b,
  //      Seconds(BATCH_WINDOW),
  //      Seconds(BATCH_WINDOW)
  //    )
  //    saveRTBDebugToDB(accumValues)
  //  }


  def computeWin(ssc: StreamingContext, component: String = "Win") {
    if (!WinSetting.enabled) return

    val stream = createUnifiedStream(WinSetting.topics, ssc)

    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeWinLog(r.value()))
     .reduceByKeyAndWindow(
        sumIntLong(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
     .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = WinDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)

            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })



  }


  def computeMWin(implicit ssc: StreamingContext, component: String = "MWin") {
    if (!MWinSetting.enabled) return
    val stream = createUnifiedStream(MWinSetting.topics,ssc)
    //val streams = createTopicStreams(MWinSetting.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]


    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeMWinLog(r.value()))
      .reduceByKeyAndWindow(
        sumIntLong(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = MWinDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)

            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })

  }

  def computeImpTracking(implicit ssc: StreamingContext, component: String = "ImpTrack") {
    if (!ImpTrackingSetting.enabled) return

    val stream = createUnifiedStream(ImpTrackingSetting.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeTrackingLog(r.value()))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = TrackDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)
            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })
  }


  def computeClkTracking(implicit ssc: StreamingContext, component: String = "ClkTrack") {
    if (!ClkTrackingSetting.enabled) return

    val stream = createUnifiedStream(ClkTrackingSetting.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeTrackingLog(r.value()))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = TrackDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)
            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })

  }


  def computeImpTrackingConv(implicit ssc: StreamingContext, component: String = "ImpTrackingConv"): Unit = {
    if (!ImpConvSetting.enabled) return
    val stream = createUnifiedStream(ImpConvSetting.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.map(r => analyzeTrackingConv(r.value()))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = TrackingConvDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)

            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })

  }


  def computeImpYdTrackingConv(implicit ssc: StreamingContext, component: String = "ImpYdTrackingConv"): Unit = {
    if (!ImpYdConvSetting.enabled) return
    val stream = createUnifiedStream(ImpYdConvSetting.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.map(r => analyzeTrackingConv(r.value()))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = TrackingConvDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)

            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })

  }

  def computeTrackingDasp(implicit ssc: StreamingContext, component: String = "TrackingDasp"): Unit = {
    if (!DaspSetting.enabled) return
    val stream = createUnifiedStream(DaspSetting.topics,ssc)


    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeTrackingDasp(r.value()))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = TrackingDaspDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)

            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })


  }

  def computeUMA(implicit ssc: StreamingContext, component: String = "UMA"): Unit = {
    if (!UMASetting.enabled) return
    val stream = createUnifiedStream(UMASetting.topics,ssc)


    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.map(r => analyzeUMA(r.value()))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = UMADecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)
            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })




  }


  def computeDaspMob(implicit ssc: StreamingContext, component: String = "TrackingYD"): Unit = {
    if (!DaspMobSetting.enabled) return
    val stream = createUnifiedStream(DaspMobSetting.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeDaspMob(r.value()))
       .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = TrackingDaspDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)

            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })


  }

  def computeDaspSDK(implicit ssc: StreamingContext, component: String = "TrackingSDK"): Unit = {
    if (!DaspSdkSetting.enabled) return
    val stream = createUnifiedStream(DaspSdkSetting.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]

    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeDaspSdk(r.value()))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))
      .foreachRDD((rdd, time) => {
        val receiveTime = SQLBuilder.toSQLTimestamp(
          time.floor(Minutes(1)).milliseconds)
        rdd.foreachPartition { partitionOfRecords =>
          val ptime = System.currentTimeMillis()
          partitionOfRecords.foreach { record =>
            val item = TrackingDaspDecoder.decode(record)
            val sql = SQLBuilder.build(item, receiveTime)
            if (sql != "") {
              println("sql -> " + sql)
              cmdActor ! SQLCMD(ptime.toString, sql)
            }
          }
          cmdActor ! EXE(ptime.toString, dbparams)
        }

        for (offset <- offsetRanges) {
          println("############## zk set offset   :" + offset.toString())
          ZkWork.znodeDataSet(s"consumers/rtm/new/${offset.topic}_${offset.partition}", s"${offset.untilOffset}")
        }

      })


  }


  def computeSegment(implicit ssc: StreamingContext, bc: Broadcast[Set[String]], component: String = "UC_Segment"): Unit = {
    if (!SegmentSettings.enabled) return
    val stream = createUnifiedStream(SegmentSettings.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]
    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeSegment(r.value(),bc))
     .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))


  }

  def computeCM(implicit ssc: StreamingContext, bc: Broadcast[Set[String]], component: String = "CookieMapping"): Unit = {
    if (!CMSettings.enabled) return


    val stream = createUnifiedStream(CMSettings.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]


    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeCM(r.value(),bc))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))


  }

  def computeRender(implicit ssc: StreamingContext, bc: Broadcast[Set[String]], component: String = "Render"): Unit = {
    if (!RenderSettings.enabled) return
    val stream = createUnifiedStream(RenderSettings.topics,ssc)

    var offsetRanges = Array.empty[OffsetRange]


    stream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.flatMap(r => analyzeRender(r.value(),bc))
      .reduceByKeyAndWindow(
        sumIntInt(_, _),
        Seconds(BATCH_WINDOW),
        Seconds(BATCH_WINDOW))

  }


  def createStreamingContext() = {
    val conf = new SparkConf()
      .setAppName(APP_NAME)
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")


    implicit val ssc = new StreamingContext(conf, Seconds(60)) // jungle comment  , streaming context set to 60

    // broadcast about
    val broadcastZids = ssc.sparkContext.broadcast(initZids)

    // broadcast id
    val BroadcastId = broadcastZids.id


    val bsys = ActorSystem("DriverBroadcase")
    //val bactor = bsys.actorOf(akka.actor.Props(classOf[UpdateBroadcast], ssc, BroadcastId),
    //  name = "broadcast_actor")

    val bactor = bsys.actorOf(akka.actor.Props(classOf[UpdateBroadcast], ssc, broadcastZids),
      name = "broadcast_actor")

    bsys.scheduler.schedule(0 seconds, 60 seconds, bactor, "update111")


    // TODO: get init zid sets

    ssc.checkpoint(checkPointDir)

    (ssc, broadcastZids)
  }


  class UpdateBroadcast(ssc: StreamingContext, broadcastZids: Broadcast[Set[String]]) extends Actor with Logging {
    val random = new Random()

    def receive = {
      case update: String => {

        println("@@ receive " + update + " broadcastZids ,size : " + broadcastZids.value.size)
        val tryZids: Try[String] = Try {

          val url = new URL(s"${CT_API_HOST}${CT_ZIDS_API}")
          val urlCon = url.openConnection()
          urlCon.setConnectTimeout(CT_TIMEOUT)
          urlCon.setReadTimeout(CT_TIMEOUT)
          fromInputStream(urlCon.getInputStream).getLines.mkString("").replace("\"", "")
        }

        tryZids.foreach({
          case contens: String => {
            val leftBracket = contens.indexOf('[')
            val rightBraket = contens.indexOf(']')

            if (rightBraket != -1 && leftBracket != -1 && (leftBracket + 1) < rightBraket) {
              val zids = contens.substring(leftBracket + 1, rightBraket).split(",").toSet
              if (initZids != zids) {
                initZids.clear()
                initZids ++= zids
                broadcastZids.unpersist
                ssc.sparkContext.broadcast(initZids)
                logInfo(s"=================> Master Update BroadCast Succuess!. Zids length: ${initZids.size}}")
              }
            } else {
              logError("Zids Content is Blank")
            }
          }
          case _ =>
        })

        tryZids.failed foreach ({
          case ex => logError(s"API ERROR: ${ex.toString}")
        })

      }
      case _ =>
    }

  }

  class UpdateBroadcastOld(ssc: StreamingContext, BroadcastId: Long) extends Actor with Logging {
    val random = new Random()

    def receive = {
      case update: String => {
        val tryZids: Try[String] = Try {

          val url = new URL(s"${CT_API_HOST}${CT_ZIDS_API}")
          val urlCon = url.openConnection()
          urlCon.setConnectTimeout(CT_TIMEOUT)
          urlCon.setReadTimeout(CT_TIMEOUT)
          fromInputStream(urlCon.getInputStream).getLines.mkString("").replace("\"", "")
        }

        tryZids.foreach({
          case contens: String => {
            val leftBracket = contens.indexOf('[')
            val rightBraket = contens.indexOf(']')

            if (rightBraket != -1 && leftBracket != -1 && (leftBracket + 1) < rightBraket) {
              val zids = contens.substring(leftBracket + 1, rightBraket).split(",").toSet
              if (initZids != zids) {
                initZids.clear()
                initZids ++= zids
                logInfo(s"=================> Master Update BroadCast Succuess!. Zids length: ${initZids.size}}")
              }
            } else {
              logError("Zids Content is Blank")
            }
          }
          case _ =>
        })

        tryZids.failed foreach ({
          case ex => logError(s"API ERROR: ${ex.toString}")
        })

      }
      case _ =>
    }

  }

  // ----- update end

  def main(args: Array[String]) {
    println("####### rtm streaming ")
    val res = createStreamingContext()
    // for product
    //    val ssc = StreamingContext.getOrCreate(checkPointDir, createStreamingContext)

    ///computeWin(ssc)
    computeRTB(res._1, res._2)
    computeWin(res._1)

    computeImpTracking(res._1)
    computeClkTracking(res._1)

    computeMWin(res._1)

    computeImpTrackingConv(res._1)
    computeImpYdTrackingConv(res._1)


    computeTrackingDasp(res._1)
    computeUMA(res._1)
    computeDaspMob(res._1)
    computeDaspSDK(res._1)
    computeSegment(res._1,res._2)
    computeCM(res._1,res._2)
    computeRender(res._1,res._2)




    res._1.start()
    res._1.awaitTermination()
  }
}

