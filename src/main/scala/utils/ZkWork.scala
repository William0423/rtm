package utils

/**
  * Created by admin on 18/8/28.
  */

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

object ZkWork {
  val TIME_OUT = 5000
  var zooKeeper: ZooKeeper = _
  var connected = false

  def watcher = new Watcher() {
    def process(event: WatchedEvent) {
      println(s"***** [ ZkWork ] process : " + event.getType)
    }
  }

  def connect() {
    if(connected == true){
       return
    }
    println(s"[ ZkWork ] zk start to connect")
    zooKeeper = new ZooKeeper("172.22.56.41:2333,172.22.56.42:2333,172.22.56.43:2333,172.22.57.116:2333,172.22.57.117:2333", TIME_OUT, watcher)
    connected = true
  }

  def znodeCreate(znode: String, data: String) {
    println(s"[ ZkWork ] zk create /$znode , $data")
    zooKeeper.create(s"/$znode", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  def znodeDataSet(znode: String, data: String) {
    println(s"[ ZkWork ] zk data set /$znode")
    zooKeeper.setData(s"/$znode", data.getBytes(), -1)
  }


  def znodeDataGet(znode: String): String = {
    connect()
    Thread.sleep(100)
    println(s"[ ZkWork ] zk data get /$znode")
    try {
      new String(zooKeeper.getData(s"/$znode", true, null), "utf-8")
    } catch {
      case _: Exception => null
    }
  }

  def znodeIsExists(znode: String): Boolean = {
    connect()
    Thread.sleep(100)
    println(s"[ ZkWork ] zk znode is exists /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => false
      case _ => true
    }
  }

  def offsetWork(znode: String, data: String) {
    connect()
    Thread.sleep(100)
    println(s"[ ZkWork ] offset work /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => znodeCreate(znode, data)
      case _ => znodeDataSet(znode, data)
    }
    println(s"[ ZkWork ] zk close★★★")
    zooKeeper.close()
  }


}