
import tracking.Tracking._

object TestTracking {
/*
	required int64 request_time = 1; //unixtime 
	required uint32 ip = 2; // 
	required int32 status= 3; // http status 
	required string uri= 4; // http request line not include param 
	required string zid= 5; // 
	required string param= 6; // 
	required string body= 7; // 
	required string ua= 8; // 
	required int64 process_time = 9; //
	required string refer= 10; //
	optional string request_id = 11; // mk request_id 
}
*/
	def main(args: Array[String]) {
		val clbuilder = common_log.newBuilder

		val mk = args(0)
		//"RK4tWie8QoHOR0EIOuG_XDE7kT_Xt2D7p0FoSUfeS3yh1Y1AMr8I6obRZUjVnQAxGFyntztr9DoeB_elSIC03znpFliPWfImrKp7BW4XFa1j7tZbbRpeukP_YI4vQZY9hMyzHFK8QOFGm6dk3cc2EP35PLVpPImL8-3o1C3MZusyGI9BlIDj6KLzr2JxuPAGi78kMNEpHcp2mLcvAguW73_GiGiGjIBB_Ocd_dRNTcWXddh6Cva56KpMrw25Bc8Hf6Ezu0q1DOVWyDVclYKe6B0cdH31VWnqB5er-fiwUne7gcveN9RLLWRKA4G3wJXwoIrKCQq9FCwcWyq0xmFd4czd1721v0S-CReyfUsWZM9TPTfuQwFYjHUyPr1KBrChNZvECJNx4CYqFyMY1ylF1GOzJQ_SkgbTic1-VSO2ityNrsCUlntZ1H9ooyzkZcQ2u7ZLdC2vw5aoQrfogsf_IJYydlO4rKL31WR9IEiftT9YMNvGZpiZYXSqeAfwEryZtXaNJdohdpqU_JduJZv_qINl3jEwmPEabWBQn8ettpCeRnp6Hh9bPu457AXtP5Y5x3IKnp5RIcwxQhu90gj5WDIPZcyP0v3Ef65wgEtZXJ0lFoyZwqej2U0z-E_7qXAlW9DWCQ5_PLyUQeRkUS9xMCisFGg2loxCRinZmZJ5wBf6f_RhI4dHe2lO8y_vZc2TCBj0FcEBk6qG5UV1wDCmWOhE16DQkjJk3G5CA-G3dcXfJbPhHuc5uGfuWFaVw8n1"
		clbuilder.setRequestTime(1234567899)
		clbuilder.setIp(0xFFFFFFFF)
		clbuilder.setStatus(200)
		clbuilder.setUri("/imp/i3")
		clbuilder.setZid("17e8c4b8ecbe1cd152a73fb25ba88d1b")
		clbuilder.setParam("p=23&mk=" +mk + "&t=2")
		clbuilder.setBody("")
		clbuilder.setUa("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36")
		clbuilder.setProcessTime(40)
		clbuilder.setRefer("http://mc.zampda.net/files/template_file/tploader3_1_source.htm")

		val cl = clbuilder.build
		val bs = new java.io.ByteArrayOutputStream()
		val header = Array[Byte](8, 0, 0, 0, 0, 0, 0, 0, 0)
		bs.write(header, 0, 9)
		cl.writeTo(bs)
		Parsers.analyzeTrackingLog(bs.toByteArray).foreach(t => println(t))
	}
}
