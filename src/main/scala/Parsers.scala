
import java.net.URLDecoder

import utils.Utils.pushPbData

import scala.collection.mutable.Set
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import com.zamplus.protobuf.uclog.IdsLog.PlatformIds
import zampdata.records.pb.Render.render_log
import zampdata.records.pb.SegmentsLog._
import zampdata.records.pb.MegatronBidrequest._
import BillingLog._
import tracking.Tracking._
import tracking.Mk._
import tracking.Logger.smartpixel_log
import dmp_tracking.Logger.{common_log => sdk_common_log}

import scala.collection.JavaConversions._
import org.apache.commons.codec.binary.Base64
import RTMStreamingAnalyze.{CT_API_HOST, CT_CM_PUSH_API, CT_RENDER_PUSH_API, CT_RTB_PUSH_API, CT_SEG_PUSH_API}
import com.zamplus.pb.ClickhouseLog
import com.zamplus.pb
import com.zamplus.pb.ClickhouseLog.CLRtb
import com.zamplus.pb.Megatornlogs4Rtm.Megatronlogs4Rtm
import org.apache.spark.internal.Logging

import scala.collection.immutable.HashMap

object Parsers extends Logging {


	def getHostName() = {
		"DEFAULT"
//		val inetAddr = java.net.InetAddress.getLocalHost()
//		inetAddr.getHostName
	}

	import ComputeTypeConversion._

	def has_chance(implicit edge: Double=1.0) = Math.random() <= edge
	



	def analyzeRTBLog(record: Array[Byte], bc: Broadcast[Set[String]]): Traversable[(String, (Int, Int))]  = {
    val results = new scala.collection.mutable.ListBuffer[(String, (Int, Int))]

/** record format:
  * -----------------------------------------
  * | length | content of header | protobuf |
  * |--------|-------------------|----------|
  * |   0-7  |       8 -- ?      | ?+1 -    | bit
  * -----------------------------------------
  */

		if (!has_chance(RTMStreamingAnalyze.CHANCE)) {
			return results
		}

//		val headerLength = record(0);
    val megatron = try {
        Megatronlogs4Rtm.parseFrom(record.drop(9))    // select all eles except first 9 ones;
      } catch {
        case e: Throwable =>
          logError("## "  + e.getMessage)
          return Array[(String, (Int, Int))](RTBInputCheck(getHostName, 0, 1))
      }

//    if (bc.value.contains(if (megatron.hasZid) megatron.getZid else "")) {
//      pushPbData(s"$CT_API_HOST$CT_RTB_PUSH_API", record.drop(9))
//    }

    def emit(r: (String, (Int, Int))) {
      results += r
    }
    for (log <- megatron.getLogsList if log.hasRequestId && log.getRequestId != "") {

      if (bc.value.contains(if (log.hasZid) log.getZid else "")) {
        pushPbData(s"$CT_API_HOST$CT_RTB_PUSH_API", record.drop(9),
          if (log.hasRequestTime) log.getRequestTime else 0)
      }

      val vendor_id = log.getVendorId

      for (had <- log.getHandledAdsList) {
        val total_error_code = if (had.hasTotalErrorCode) had.getTotalErrorCode else -1
				
				
        emit(RTBVendorErrorCode(vendor_id, total_error_code, 1))

        if (total_error_code == 0) {
          emit(RTBVendor(vendor_id, 1, 1))
        } else {
          emit(RTBVendor(vendor_id, 1, 0))
        }

        for (bid <- had.getBidCodeList) {
          val cap_adgroup_id = if (bid.hasAdgroupId) bid.getAdgroupId else 0
          val cap_error_code = if (bid.hasErrorCode) bid.getErrorCode else -1

//          val ec_str = "E%03d".format(cap_error_code)

          emit(RTBAdgroupErrorCode(vendor_id, cap_adgroup_id, cap_error_code, 1))
          if (cap_error_code == 0) {
            emit(RTBAdgroup(vendor_id, cap_adgroup_id, 1, 1))
          } else {
            emit(RTBAdgroup(vendor_id, cap_adgroup_id, 1, 0))
          }
        }
      }
    }
    emit(RTBCount(getHostName, 1, results.size))
    emit(RTBInputCheck(getHostName, 1, 0))
    results
	}


	/**
	def analyzeRTBLog(record: Array[Byte], bc: Broadcast[Set[String]]): Traversable[(String, (Int, Int))]  = {
		val results = new scala.collection.mutable.ListBuffer[(String, (Int, Int))]

		/** record format:
			* -----------------------------------------
			* | length | content of header | protobuf |
			* |--------|-------------------|----------|
			* |   0-7  |       8 -- ?      | ?+1 -    | bit
			* -----------------------------------------
			*/

		if (!has_chance(RTMStreamingAnalyze.CHANCE)) {
			return results
		}

		//		val headerLength = record(0);
		val log = try {
			CLRtb.parseFrom(record.drop(9))    // select all eles except first 9 ones;
		} catch {
			case e: Throwable =>
				logError(e.getMessage)
				return Array[(String, (Int, Int))](RTBInputCheck(getHostName, 0, 1))
		}

		//    if (bc.value.contains(if (megatron.hasZid) megatron.getZid else "")) {
		//      pushPbData(s"$CT_API_HOST$CT_RTB_PUSH_API", record.drop(9))
		//    }

		def emit(r: (String, (Int, Int))) {
			results += r
		}


		if (bc.value.contains(log.getZid)) {
			pushPbData(s"$CT_API_HOST$CT_RTB_PUSH_API", record.drop(9),
				log.getTimestamp)
		}

		val vendor_id = log.getVendorId

		val total_error_code = log.getTotalErrorCode


		emit(RTBVendorErrorCode(vendor_id, total_error_code, 1))

		if (total_error_code == 0) {
			emit(RTBVendor(vendor_id, 1, 1))
		} else {
			emit(RTBVendor(vendor_id, 1, 0))
		}


		val errorMap  = log.getAdgroupErrorCodeMap();
		for( i <- errorMap){

			val cap_adgroup_id = i._1
			val cap_error_code = i._2
			emit(RTBAdgroupErrorCode(vendor_id, cap_adgroup_id, cap_error_code, 1))
			if (cap_error_code == 0) {
				emit(RTBAdgroup(vendor_id, cap_adgroup_id, 1, 1))
			} else {
				emit(RTBAdgroup(vendor_id, cap_adgroup_id, 1, 0))
			}

		}

		emit(RTBCount(getHostName, 1, results.size))
		emit(RTBInputCheck(getHostName, 1, 0))

		results
	}
	***/

	def analyzeRTBDebugLog(record: Array[Byte]): (String, Array[(String, String, String)]) = {
		val E = "ERROR"
		def corruptInput = (E, Array((E, E, E)))
		val rtb_debug_log = try {
			RtbLog.parseFrom(record.drop(9)).getLogs(0)
//			common_log.parseFrom(record.drop(9))
		} catch {
			case e: Throwable =>
				logError(e.getMessage)
				return corruptInput
		}

		val zid = rtb_debug_log.getZid
//		val bid_data, res_data, meg_data = "{\"test\":1}"
		val bid_data = rtb_debug_log.getBidData
		val res_data = rtb_debug_log.getResponseData
		val meg_data = rtb_debug_log.getMegatronInnerData
		(zid, Array((bid_data, res_data, meg_data)))
	}

	def analyzeWinLog(record: Array[Byte]): Traversable[(String, (Int, Long))] = {
		def corruptInput = Array[(String, (Int, Long))](WinInputCheck(getHostName, 0, 1))

		val billing = try {
				billing_log.parseFrom(record.drop(9))
			} catch {
	    		case e: Throwable =>
					logError(e.getMessage)
	    			return corruptInput
	    	}


		val vendor_id = billing.getVendorId
		val adgroup_id = billing.getAdgroupId
		if (adgroup_id == 0 || billing.getLogType() != "w") {
			return Array[(String, (Int, Long))]()
		}
		
		val win_price = billing.getWinPrice
		val ecode = billing.getParseCode


		Array[(String, (Int, Long))](
			WinInputCheck(getHostName, 1, 0),
			WinVendor(vendor_id, 1, win_price),
			WinErrorCode(vendor_id, adgroup_id, ecode, 1),
			WinNotice(vendor_id, adgroup_id, 1, win_price)
		)
	}

	def analyzeMWinLog(record: Array[Byte]): Traversable[(String, (Int, Long))] = {
		def corruptInput = Array[(String, (Int, Long))](MWinInputCheck(getHostName, 0, 1))

		val billing = try {
			billing_log.parseFrom(record.drop(9))
		} catch {
			case e: Throwable =>
				logError(e.getMessage)
				return corruptInput
		}

		val vendor_id = billing.getVendorId
		val adgroup_id = billing.getAdgroupId
		if (adgroup_id == 0 || billing.getLogType() != "w") {
			return Array[(String, (Int, Long))]()
		}

		val win_price = billing.getWinPrice
		val ecode = billing.getParseCode


		Array[(String, (Int, Long))](
			MWinInputCheck(getHostName, 1, 0),
			MWinVendor(vendor_id, 1, win_price),
			MWinNotice(vendor_id, adgroup_id, 1, win_price)
		)
	}
	
	val T_PB_MK_KEY = "_ABSTracking2014"
	val PB_MK_KEY = "ABSTracking2014"

	def explodeParams(param: String, del: String="&"): Map[String, String] = {
		param.split(del).map({
      _.split("=") match {
        case entry if entry.size == 1 => (entry(0), "")
        case entry => (entry(0), entry(1))
      }
		}).toMap
	}

	def analyzeTrackingLog(record: Array[Byte]): Traversable[(String, (Int, Int))] = {
		def corruptInput = Array[(String, (Int, Int))](TrackInputCheck(getHostName, 0, 1))
		if (record.size == 0) {
			return corruptInput
		}
		val log = try {
				common_log.parseFrom(record.drop(9))
//        common_log.parseFrom(record.drop(9))
			} catch {
	    		case e: Throwable =>
					logError(e.getMessage)
	    			return corruptInput
			}

		if (!log.hasRequestId || log.getRequestId == "") {
			return corruptInput
		}

		val params = explodeParams(log.getParam)
		if (!params.contains("mk") && !params.contains("ext")) {
			return corruptInput
		}
		val mk_str = if (params.contains("mk")) params("mk") else params("ext")

		val mkObj = try {
			val decoded_mk = Blowfish.decode(Base64.decodeBase64(mk_str.getBytes))
			mk.parseFrom(decoded_mk)
		} catch {
			case e: Throwable =>
				logError(e.getMessage)
				return corruptInput
		}
		val vendor_id = mkObj.getVendorId
		val adgroup_id = mkObj.getAdgroupId
		val adslot_id = mkObj.getAdslotId
		val secret_key = mkObj.getSecretKey
		if (secret_key != T_PB_MK_KEY && secret_key != PB_MK_KEY) {
			return corruptInput
		}

		val account_id = mkObj.getAccountId

		val uri = log.getUri

		val pv = if (uri == "/imp/i3") 1 else 0
		val click = if (uri == "/tms/c3") 1 else 0

		Array[(String, (Int, Int))](
			TrackInputCheck(getHostName, 1, 0),
			TrackVendor(vendor_id, pv, click),
			TrackAdgroup(vendor_id, account_id, adgroup_id, pv, click)
		)
	}

	// 2015-05-16 added by Joey.Chang


	def analyzeTrackingConv(record: Array[Byte]): (Int, (Int, Int)) = {

    val errorOutput = (0, (0, 1))  // 0 for error
    val log = try {
      common_log.parseFrom(record.drop(9))
//      common_log.parseFrom(record.drop(9))
    } catch {
      case e: Throwable =>
        logError(e.getMessage)
        return errorOutput
    }

    val params = explodeParams(log.getParam)
    if (!params.contains("a")) {
      logError("params doesn't contains: account_id!")
      return errorOutput
    }
		
		
		
    val account_id = try {
      params("a").toInt
    } catch {
      case e: Throwable =>
        logError(e.getMessage)
        return errorOutput
    }

    if (log.hasZid) (account_id, (1, 1)) else (account_id, (0, 1))

  }


  def analyzeTrackingDasp(record: Array[Byte]): Traversable[(String, (Int, Int))] = {

		val results = new scala.collection.mutable.ListBuffer[(String, (Int, Int))]

		def emit(r: (String, (Int, Int))) {
			results += r
		}
		
    val errorOutput: (String, (Int, Int)) = TrackingDasp(0, 0, 1)  // 0 for error
    val log = try {
        smartpixel_log.parseFrom(record.drop(9))
//        smartpixel_log.parseFrom(record.drop(9))
      } catch {
        case e: Throwable =>
          logError(e.getMessage)
					emit(errorOutput)
          return results
      }
		
    val params = explodeParams(log.getParam)
    val account_id = try {
      params("a").toInt
    } catch {
      case e: Throwable =>
        logError(e.getMessage)
				emit(errorOutput)
        return results
    }
		
		val ext_args = try {
			URLDecoder.decode(params.getOrElse("ext_args", ""), "UTF-8")
		} catch {
			case e: Throwable => 
				logWarning("Can't decode ext_args: " + params.getOrElse("ext_args", ""))
				""
		}
			val ext_args_map = explodeParams(ext_args, ";")
			for ((k, v) <- ext_args_map) {

				if (k != "") emit(TrackingDaspArgs(account_id, k))
			}

			if(params.contains("rid")) emit(TrackingDasp(account_id, 1, 1)) else emit(TrackingDasp(account_id, 0, 1))

			results
	}
  

	
	def analyzeUMA(record: Array[Byte]): (String, Int) = {
		val errorOutput = ("", 0)
		val log = try {
			common_log.parseFrom(record.drop(9))
		} catch {
			case e: Throwable =>
				logError(e.getMessage)
				return errorOutput
		}
		
		val params = explodeParams(log.getParam)
		val adslot_id = params.getOrElse("s", "")
		val c = params.getOrElse("c", "N/A")
		if (adslot_id != "" && (c == "" || c == "-1")) (adslot_id, 1)
		else errorOutput

	}

//	def analyzeDaspMob(record: Array[Byte]): (Int, (Int, Int)) = {
//
//		val errorOutput = (0, (0, 1))  // 0 for error
//		val log = try {
//			common_log.parseFrom(record.drop(9))
//		} catch {
//			case e: Throwable =>
//				logError(e.getMessage)
//				return errorOutput
//		}
//
//		val params = explodeParams(log.getParam)
//		if (!params.contains("a")) {
//			logError("params doesn't contains: account_id!")
//			return errorOutput
//		}
//
//
//
//		val account_id = try {
//			params("a").toInt
//		} catch {
//			case e: Throwable =>
//				logError(e.getMessage)
//				return errorOutput
//		}
//
//		if(params.contains("rid")) (account_id, (1, 1)) else (account_id, (0, 1))
//
//	}

	def analyzeDaspMob(record: Array[Byte]): Traversable[(String, (Int, Int))] = {
		val results = new scala.collection.mutable.ListBuffer[(String, (Int, Int))]

		def emit(r: (String, (Int, Int))) {
			results += r
		}

		val errorOutput: (String, (Int, Int)) = TrackingDasp(0, 0, 1)  // 0 for error
		val log = try {
				common_log.parseFrom(record.drop(9))
				//        smartpixel_log.parseFrom(record.drop(9))
			} catch {
				case e: Throwable =>
					logError(e.getMessage)
					emit(errorOutput)
					return results
			}

		val params = explodeParams(log.getParam)
		val account_id = try {
			params("a").toInt
		} catch {
			case e: Throwable =>
				logError(e.getMessage)
				emit(errorOutput)
				return results
		}

		val ext_args = try {
			URLDecoder.decode(params.getOrElse("ext_args", ""), "UTF-8")
		} catch {
			case e: Throwable =>
				logWarning("Can't decode ext_args: " + params.getOrElse("ext_args", ""))
				""
		}
		val ext_args_map = explodeParams(ext_args, ";")
		for ((k, v) <- ext_args_map) {

			if (k != "") emit(TrackingDaspArgs(account_id, k))
		}

		if(params.contains("rid")) emit(TrackingDasp(account_id, 1, 1)) else emit(TrackingDasp(account_id, 0, 1))

		results
	}

	final val APP_KEY_MAP = HashMap[String , Int](
		"CAE5F076BC6FDC35899191A0D38E3308"->285, 
		"8D4C14E556D4078AC038EF8583BD665E"->285, 
		"58A5ABD4FD59E3B953756FAA23F2042D"->432,
		"FADFED4960E4904BCC618A853FE0FF3B"->432
	)
	
	def analyzeDaspSdk(record: Array[Byte]): Traversable[(String, (Int, Int))] = {
		val results = new scala.collection.mutable.ListBuffer[(String, (Int, Int))]

		def emit(r: (String, (Int, Int))) {
			results += r
		}
		val errorOutput: (String, (Int, Int)) = TrackingDasp(0, 0, 1)  // 0 for error
		val log = try {
				sdk_common_log.parseFrom(record.drop(9))
				//        smartpixel_log.parseFrom(record.drop(9))
			} catch {
				case e: Throwable =>
					logError(e.getMessage)
					emit(errorOutput)
					return results
			}

		val params = explodeParams(log.getParam)
		val account_id = try {
			params("appkey").split("_").toList match {
				case head::Nil => APP_KEY_MAP.getOrElse(head, 0)
				case head::account_id::Nil => account_id.toInt
				case _ => throw new Exception("No Account")	
			}
		} catch {
			case e: Throwable =>
				logError(e.getMessage)
				emit(errorOutput)
				return results
		}

		val ext_args = try {
			URLDecoder.decode(params.getOrElse("ext_args", ""), "UTF-8")
		} catch {
			case e: Throwable =>
				logWarning("Can't decode ext_args: " + params.getOrElse("ext_args", ""))
				""
		}
		val ext_args_map = explodeParams(ext_args, ";")
		for ((k, v) <- ext_args_map) {

			if (k != "") emit(TrackingDaspArgs(account_id, k))
		}
		
		results
	}

  def analyzeSegment(record: Array[Byte], bc: Broadcast[Set[String]]): Traversable[(String, (Int, Int))] = {

    val results = new scala.collection.mutable.ListBuffer[(String, (Int, Int))]
    val log = try {
      UcSegments.parseFrom(record.drop(9))
        //        smartpixel_log.parseFrom(record.drop(9))
      } catch {
        case e: Throwable =>
          logError(e.getMessage)
          return results
      }

    val zid = if (log.hasZid) log.getZid else ""

    if (bc.value.contains(zid)) {
      pushPbData(s"$CT_API_HOST$CT_SEG_PUSH_API", record.drop(9))
    }

    results
  }

  def analyzeRender(record: Array[Byte], bc: Broadcast[Set[String]]): Traversable[(String, (Int, Int))] = {

    val results = new scala.collection.mutable.ListBuffer[(String, (Int, Int))]
    val log = try {
      render_log.parseFrom(record.drop(9))
      //        smartpixel_log.parseFrom(record.drop(9))
    } catch {
      case e: Throwable =>
        logError(e.getMessage)
        return results
    }

    val zid = if (log.hasZid) log.getZid else ""

    if (bc.value.contains(if (log.hasZid) log.getZid else "")) {
      pushPbData(s"$CT_API_HOST$CT_RENDER_PUSH_API", record.drop(9))
    }

    results
  }

  def analyzeCM(record: Array[Byte], bc: Broadcast[Set[String]]): Traversable[(String, (Int, Int))] = {
    val results = new scala.collection.mutable.ListBuffer[(String, (Int, Int))]
    val log = try {
      PlatformIds.parseFrom(record.drop(9))
      //        smartpixel_log.parseFrom(record.drop(9))
    } catch {
      case e: Throwable =>
        logError(e.getMessage)
        return results
    }

    val zid = if (log.hasZid) log.getZid else ""
    if (bc.value.contains(zid)) {
      pushPbData(s"$CT_API_HOST$CT_CM_PUSH_API", record.drop(9))
    }

    results
  }

}
