import org.apache.spark.internal.Logging
//import log.Logging

class Item

case class RTBVendor(vendor: Int, pv: Int, bid: Int) extends Item {
	def encode(): (String, (Int, Int)) = ("V" + vendor, (pv, bid))
}

case class RTBAdgroup(vendor: Int, adgroup: Long, pv: Int, bid: Int) extends Item {
	def encode(): (String, (Int, Int)) = ("B" + vendor + "|" + adgroup, (pv, bid))
}

case class RTBAdgroupErrorCode(vendor: Int, adgroup: Long, errcode: Long, count: Int) extends Item {
	def encode(): (String, (Int, Int)) = ("E" + vendor + "|" + adgroup + "|" + errcode, (count, 0))
}

case class RTBVendorErrorCode(vendor: Int, errcode: Long, count: Int) extends Item {
	def encode(): (String, (Int, Int)) = {
		/**
		  * please make sure there is no 'empty' errcode.
		  */
		("A" + vendor + "|" + errcode, (count, 0))
	}
}

case class RTBDomain(vendor: Int, domain: String, pv: Int, count: Int) extends Item {
	def encode(): (String, (Int, Int)) = {
		/**
		  * please make sure there is no 'empty' domain.
		  */
		("P" + vendor + "|" + domain, (pv, count))
	}
}

case class RTBCount(host: String, input: Int, output: Int) extends Item {
	def encode(): (String, (Int, Int)) = ("C" + host, (input, output))
}

case class RTBInputCheck(host: String, passed: Int, corrupt: Int) extends Item {
	def encode(): (String, (Int, Int)) = ("-" + host, (passed, corrupt))
}

object RTBDecoder {
	def decode(r: (String, (Int, Int))): Item = {
		try {
			val (k, v) = r
			k(0) match {
				case 'C' => 
					RTBCount(k.substring(1), v._1, v._2)
				case 'V' => 
					RTBVendor(k.substring(1).toInt, v._1, v._2)
				case 'B' => {
					val part = k.substring(1).split("\\|")
					RTBAdgroup(part(0).toInt, part(1).toLong, v._1, v._2)
				}
				case 'E' =>  {
					val part = k.substring(1).split("\\|", 3)
					RTBAdgroupErrorCode(part(0).toInt, part(1).toLong, part(2).toInt, v._1)
				}
				case 'A' =>  {
					val part = k.substring(1).split("\\|")
					RTBVendorErrorCode(part(0).toInt, part(1).toInt, v._1)
				}
				case 'P' => {
					val part = k.substring(1).split("\\|")
					RTBDomain(part(0).toInt, part(1), v._1, v._2)
				}
				case '-' => 
					RTBInputCheck(k.substring(1), v._1, v._2)
			}
		} catch {
			case e: NumberFormatException =>
				throw new NumberFormatException(r.toString)
		}
	}
}

case class WinVendor(vendor: Int, win: Int, cost: Long) extends Item {
	def encode(): (String, (Int, Long)) = ("V" + vendor, (win, cost))
}

case class WinNotice(vendor: Int, adgroup: Long, win: Int, cost: Long) extends Item {
	def encode(): (String, (Int, Long)) = ("W" + vendor + "|" + adgroup, (win, cost))
}

case class WinPublisher(vendor: Int, adgroup: Long, domain: String, adslot: String, win: Int, cost: Long) extends Item {
	def encode(): (String, (Int, Long)) = ("P" + vendor + "|" + adgroup + "|" + domain + "|" + adslot, (win, cost))
}

case class WinErrorCode(vendor: Int, adgroup: Long, errcode: String, count: Int) extends Item {
	def encode(): (String, (Int, Long)) = ("E" + vendor + "|" + adgroup + "|" + errcode, (count, 0))
}

case class WinMedia(vendor: Int, domain: String, channel: String, adslot: String, win: Int, cost: Long) extends Item {
	def encode(): (String, (Int, Long)) = ("M" + vendor + "|" + domain + "|" + channel + "|" + adslot, (win, cost))
}

case class WinInputCheck(host: String, passed: Int, corrupt: Int) extends Item {
	def encode(): (String, (Int, Long)) = ("-" + host, (passed, corrupt))
}

object WinDecoder {
	def decode(r: (String, (Int, Long))): Item = {
		try {
			val (k, v) = r
			k(0) match {
				case 'V' => 
					WinVendor(k.substring(1).toInt, v._1, v._2)
				case 'W' => {
					val part = k.substring(1).split("\\|")
					WinNotice(part(0).toInt, part(1).toLong, v._1, v._2)
				}
				case 'P' => {
					val part = k.substring(1).split("\\|", 4)
					WinPublisher(part(0).toInt, part(1).toLong, part(2), part(3), v._1, v._2)
				}
				case 'E' =>  {
					val part = k.substring(1).split("\\|", 3)
					WinErrorCode(part(0).toInt, part(1).toLong, part(2), v._1)
				}
				case 'M' => {
					val part = k.substring(1).split("\\|", 4)
					WinMedia(part(0).toInt, part(1), part(2), part(3), v._1, v._2)
				}
				case '-' => 
					WinInputCheck(k.substring(1), v._1, v._2.toInt)
			}
		} catch {
			case e: NumberFormatException =>
				throw new NumberFormatException(r.toString)
		}
	}
}

// model win. win-price for model

case class MWinVendor(vendor: Int, win: Int, cost: Long) extends Item {
	def encode(): (String, (Int, Long)) = ("V" + vendor, (win, cost))
}

case class MWinNotice(vendor: Int, adgroup: Long, win: Int, cost: Long) extends Item {
	def encode(): (String, (Int, Long)) = ("W" + vendor + "|" + adgroup, (win, cost))
}

case class MWinInputCheck(host: String, passed: Int, corrupt: Int) extends Item {
	def encode(): (String, (Int, Long)) = ("-" + host, (passed, corrupt))
}

object MWinDecoder {
	def decode(r: (String, (Int, Long))): Item = {
		try {
			val (k, v) = r
			k(0) match {
				case 'V' =>
					MWinVendor(k.substring(1).toInt, v._1, v._2)
				case 'W' => {
					val part = k.substring(1).split("\\|")
					MWinNotice(part(0).toInt, part(1).toLong, v._1, v._2)
				}
				case '-' =>
					MWinInputCheck(k.substring(1), v._1, v._2.toInt)
			}
		} catch {
			case e: NumberFormatException =>
				throw new NumberFormatException(r.toString)
		}
	}
}

//
case class TrackVendor(vendor: Int, pv: Int, click: Int) extends Item {
	def encode(): (String, (Int, Int)) = {
		("V" + vendor, (pv, click))
	}
}

case class TrackAdgroup(vendor: Int, account_id: Int, adgroup: Long, pv: Int, click: Int) extends Item {
	def encode(): (String, (Int, Int)) = ("A" + vendor + "|" + account_id + "|" + adgroup, (pv, click))
}

case class TrackInputCheck(host: String, passed: Int, corrupt: Int) extends Item {
	def encode(): (String, (Int, Int)) = ("-" + host, (passed, corrupt))
}

object TrackDecoder {
	def decode(r: (String, (Int, Int))): Item = {
		try {
			val (k, v) = r
			k(0) match {
				case 'V' => 
					TrackVendor(k.substring(1).toInt, v._1, v._2)
				case 'A' => {
					val part = k.substring(1).split("\\|")
					TrackAdgroup(part(0).toInt, part(1).toInt, part(2).toLong, v._1, v._2)
				}
				case '-' => 
					TrackInputCheck(k.substring(1), v._1, v._2)
			}
		} catch {
			case e: NumberFormatException =>
				throw new NumberFormatException(r.toString)
		}
	}
}

case class TrackingConv(account_id: Int, zidpv: Int, pv: Int) extends Item

object TrackingConvDecoder {
  def decode(r: (Int, (Int, Int))): TrackingConv = TrackingConv(r._1, r._2._1, r._2._2)
}

case class TrackingDaspMob(account_id: Int, rpv: Int, pv: Int) extends Item

object TrackingDaspMobDecoder {
	def decode(r: (Int, (Int, Int))): TrackingDaspMob = TrackingDaspMob(r._1, r._2._1, r._2._2)
}

case class TrackingDasp(account_id: Int, rpv: Int, pv: Int) extends Item {
	def encode(): (String, (Int, Int)) = ("D" + account_id, (rpv, pv))
}

case class TrackingDaspArgs(account_id: Int, key: String) extends Item {
	def encode(): (String, (Int, Int)) = ("A" + account_id + "|" + key, (0, 0))
}

object TrackingDaspDecoder extends Logging{
	
	def decode(r: (String, (Int, Int))): Item = {
		try {
			val (k, v) = r
			k(0) match {
				case 'D' =>
					TrackingDasp(k.substring(1).toInt, v._1, v._2)
				case 'A' => {
					val part = k.substring(1).split("\\|", 2)
					TrackingDaspArgs(part(0).toInt, part(1))
				}
			}
		} catch {
			case e: NumberFormatException =>
				throw new NumberFormatException(r.toString)
		}
	}
}

case class UMAItem(adslot_id: String, imp: Int) extends Item

object UMADecoder {
	def decode(r: (String, Int)): UMAItem = UMAItem(r._1, r._2)
}

object ComputeTypeConversion {
	type StringIntInt = (String, (Int, Int))
	type StringIntLong = (String, (Int, Long))	

	/**
	  * impliciit type conversions.
	  */
	implicit def RTBVendorToStringIntInt(i: RTBVendor): StringIntInt = i.encode
	implicit def RTBAdgroupToStringIntInt(i: RTBAdgroup): StringIntInt = i.encode
	implicit def RTBAdgroupErrorCodeToStringIntInt(i: RTBAdgroupErrorCode): StringIntInt = i.encode
	implicit def RTBVendorErrorCodeToStringIntInt(i: RTBVendorErrorCode): StringIntInt = i.encode
	implicit def RTBCountToStringIntInt(i: RTBCount): StringIntInt = i.encode
	implicit def RTBInputToStringIntInt(i: RTBInputCheck): StringIntInt = i.encode

	// Model win
	implicit def MWinVendorToStringIntLong(i:MWinVendor): StringIntLong = i.encode
	implicit def MWinNoticeToStringIntLong(i:MWinNotice): StringIntLong = i.encode
	implicit def MWinInputToStringIntLong(i: MWinInputCheck): StringIntLong = i.encode
	
	implicit def WinVendorToStringIntLong(i:WinVendor): StringIntLong = i.encode
	implicit def WinNoticeToStringIntLong(i:WinNotice): StringIntLong = i.encode
	implicit def WinErrorCodeToStringIntLong(i:WinErrorCode): StringIntLong = i.encode
	implicit def WinInputToStringIntLong(i: WinInputCheck): StringIntLong = i.encode

	implicit def TrackVendorToStringIntInt(i:TrackVendor): StringIntInt = i.encode
	implicit def TrackAdgroupToStringIntInt(i:TrackAdgroup): StringIntInt = i.encode
	implicit def TrackInputToStringIntInt(i: TrackInputCheck): StringIntInt = i.encode
	
	implicit def TrackingDaspToStringIntInt(i: TrackingDasp): StringIntInt = i.encode
	implicit def TrackingDaspArgsToStringIntInt(i: TrackingDaspArgs): StringIntInt = i.encode
}
