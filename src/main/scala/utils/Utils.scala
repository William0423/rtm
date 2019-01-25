package utils

import java.net.URL

import scala.concurrent.Future
import scala.io.Source._
import scala.util.Try
import scala.util.control.{ControlThrowable, NonFatal}
import java.io._

import org.apache.commons.codec.binary.Base64
import org.apache.spark.internal.Logging

import scala.concurrent.ExecutionContext.Implicits.global



/**
 * Created by changjian on 2016/1/19.
 */
object Utils extends Logging{

  def getUsedTimeMs(startTimeMs: Long): String = {
    " " + (System.currentTimeMillis - startTimeMs) + " ms"
  }

  def tryOrIOException(block: => Unit) {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }

  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }


  def pushPbData(api: String, data: Array[Byte]) = {

    val pushOpt = Future {
      val url = new URL(api)
      val urlCon = url.openConnection()
      urlCon.setConnectTimeout(1000)
      urlCon.setDoOutput(true);
      val out = new OutputStreamWriter(urlCon.getOutputStream(), "UTF-8")
      val raw_data = Base64.encodeBase64URLSafeString(data)
      val content = s"""{
                      | "request":[
                      | 	{"raw_data":"${raw_data}"}
                      | ]
                      |}""".stripMargin
      out.write(content) // 向页面传递数据。post的关键所在！

      out.flush()
      out.close()
      var sCurrentLine:String= ""
      val sTotalString=new StringBuffer()
      val l_urlStream= urlCon.getInputStream()
      val l_reader = new BufferedReader(new InputStreamReader(l_urlStream));
      sCurrentLine = l_reader.readLine()
      while (sCurrentLine != null) {
        sTotalString.append(sCurrentLine)
        sCurrentLine = l_reader.readLine()
      }

      sTotalString

    }
    pushOpt.failed foreach ({
      case ex => logError(s"Push data to api: $api Failed! Ex: ${ex.toString}")
    })

    pushOpt foreach({
      case info => logError(s"Push data to api $api Succuss! Response info $info")
    })

  }

  def pushPbData(api: String, data: Array[Byte], ts: Long): Unit = {
    val latency = System.currentTimeMillis() - ts * 1000
    if (latency > 10000L) {
      logWarning(s"One rtb request. Latency: $latency ms.")
    }
    pushPbData(api, data)
  }


}
