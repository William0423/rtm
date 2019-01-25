
object SQLBuilder {

	val t_rtb_all_error_codes_insert =
		"INSERT INTO `t_rtb_all_error_codes` " +
	    "(data_time, vendor_id, error_code, add_up, partial, update_time)" +
	    "VALUES ('%s', %s, '%s', %d, %d, NOW())"

	val t_rtb_error_codes_insert = 
		"INSERT INTO `t_rtb_error_codes` " +
		"    (data_time, vendor_id, adgroup_id, error_code, add_up, partial, update_time) " +
		"VALUES ('%s', %s, %s, '%s', %d, %d, NOW())"

	val t_rtb_bidding =
		"INSERT INTO `t_rtb_bidding` " +
		"    (data_time, vendor_id, adgroup_id, capt, bid, partial, update_time) " +
		"VALUES ('%s', %s, %s, %d, %d, %d, NOW())"

	val t_rtb_vendor = 
		"INSERT INTO `t_rtb_vendor` " +
		"    (data_time, vendor_id, pv, bid, partial, update_time) " +
		"VALUES ('%s', %d, %d, %d, %d, NOW())"

	val t_win_error_codes_insert = 
		"INSERT INTO `t_win_error_codes` " +
		"    (data_time, vendor_id, adgroup_id, error_code, add_up, partial, update_time) " +
		"VALUES ('%s', %s, %s, '%s', %d, %d, NOW())"

	val t_win_notice_insert =
		"INSERT INTO `t_win_notice` " +
		"    (data_time, vendor_id, adgroup_id, win, cost, partial, update_time) " +
		"VALUES ('%s', %s, %s, %d, %d, %d, NOW())"

	val t_win_vendor_insert =
		"INSERT INTO `t_win_vendor` " +
		"    (data_time, vendor_id, win, cost, partial, update_time) " +
		"VALUES ('%s', %s, %d, %d, %d, NOW())"

	// model win
	val t_mwin_notice_insert =
		"INSERT INTO `t_model_win_notice` " +
			"    (data_time, vendor_id, adgroup_id, win, cost, partial, update_time) " +
			"VALUES ('%s', %s, %s, %d, %d, %d, NOW())"

	val t_mwin_vendor_insert =
		"INSERT INTO `t_model_win_vendor` " +
			"    (data_time, vendor_id, win, cost, partial, update_time) " +
			"VALUES ('%s', %s, %d, %d, %d, NOW())"
	//
	val t_tracking_insert = 
		"INSERT INTO `t_tracking` " +
		"    (data_time, vendor_id, advertiser_id, adgroup_id, pv, click, partial, update_time) " +
		"VALUES ('%s', %s, %s, %d, %d, %d, %d, NOW())"

	val t_tracking_vendor_insert =
		"INSERT INTO `t_tracking_vendor` " +
		"    (data_time, vendor_id, pv, click, partial, update_time) " +
		"VALUES ('%s', %s, %d, %d, %d, NOW())"

	val t_rtb_count_insert =
		"INSERT INTO `t_rtb_count` " +
		"    (data_time, host, input, output, update_time) " +
		"VALUES ('%s', '%s', %d, %d, NOW())"

	val t_input_check_insert =
		"INSERT INTO `t_input_check` " +
		"    (data_time, host, source, passed, corrupt, update_time) " +
		"VALUES ('%s', '%s', '%s', %d, %d, NOW())"

  val t_tracking_conv_insert =
    "INSERT INTO `t_tracking_conv` " +
      "    (data_time, advertiser_id, zidpv, pv, partial, update_time) " +
      "VALUES ('%s', %d, %d, %d, 0, NOW())"

  val t_tracking_dasp_insert =
    "INSERT INTO `t_tracking_dasp` " +
      "    (data_time, advertiser_id, rpv, pv, partial, update_time) " +
      "VALUES ('%s', %d, %d, %d, 0, NOW())"

	val t_tracking_dasp_mob_insert =
		"INSERT INTO `t_tracking_dasp` " +
			"    (data_time, advertiser_id, `type`, rpv, pv, partial, update_time) " +
			"VALUES ('%s', %d, 2, %d, %d, 0, NOW())"

	val t_rtb_debug_insert =
		"INSERT INTO `t_rtb_debug` " +
			"    (data_time, zid, bid_data, response_data, megatron_data) " +
			"VALUES ('%s', '%s', '%s', '%s', '%s')"

	val t_uma_skylight_insert =
		"INSERT INTO `t_uma_skylight` " +
			"    (data_time, adslot_id, imp, update_time) " +
			"VALUES ('%s', '%s', %d, NOW())"
	
	val t_tracking_dasp_args_insert =
		"REPLACE INTO `t_tracking_dasp_args` " +
			"    (data_time, advertiser_id, args_key, update_time) " +
			"VALUES ('%s', %d, '%s', NOW())"
	
	def toSQLTimestamp(millissecs: Long): String = {
		new java.sql.Timestamp(millissecs).toString
	}

	def build(r: Item, time: String) = {
		r match {
			case rc: RTBCount => t_rtb_count_insert.format(time, rc.host, rc.input, rc.output)
			case rv: RTBVendor => t_rtb_vendor.format(time, rv.vendor, rv.pv, rv.bid, 0)
			case ra: RTBAdgroup => t_rtb_bidding.format(time, ra.vendor, ra.adgroup, ra.pv, ra.bid, 0)
			case rae: RTBAdgroupErrorCode => 
				t_rtb_error_codes_insert.format(time, rae.vendor, rae.adgroup, rae.errcode, rae.count, 0)
			case rve: RTBVendorErrorCode =>
				t_rtb_all_error_codes_insert.format(time, rve.vendor, rve.errcode, rve.count, 0)
			case wv: WinVendor => t_win_vendor_insert.format(time, wv.vendor, wv.win, wv.cost, 0)
			case wn: WinNotice => t_win_notice_insert.format(time, wn.vendor, wn.adgroup, wn.win, wn.cost, 0)
			case we: WinErrorCode => t_win_error_codes_insert.format(time, we.vendor, we.adgroup, we.errcode, we.count, 0)
			case tv: TrackVendor => t_tracking_vendor_insert.format(time, tv.vendor, tv.pv, tv.click, 0)
			case ta: TrackAdgroup => t_tracking_insert.format(time, ta.vendor, ta.account_id, ta.adgroup, ta.pv, ta.click, 0)

			case wv: MWinVendor => t_mwin_vendor_insert.format(time, wv.vendor, wv.win, wv.cost, 0)
			case wn: MWinNotice => t_mwin_notice_insert.format(time, wn.vendor, wn.adgroup, wn.win, wn.cost, 0)
				
			case ri: RTBInputCheck => t_input_check_insert.format(time, ri.host, "rtb", ri.passed, ri.corrupt)
			case wi: WinInputCheck=> t_input_check_insert.format(time, wi.host, "win", wi.passed, wi.corrupt)
			case ti: TrackInputCheck => t_input_check_insert.format(time, ti.host, "track", ti.passed, ti.corrupt)
			case ti: MWinInputCheck => t_input_check_insert.format(time, ti.host, "mwin", ti.passed, ti.corrupt)
      // conv
      case tc: TrackingConv => t_tracking_conv_insert.format(time, tc.account_id, tc.zidpv, tc.pv)
      case td: TrackingDasp => t_tracking_dasp_insert.format(time, td.account_id, td.rpv, td.pv)
			case tdm: TrackingDaspMob => t_tracking_dasp_mob_insert.format(time, tdm.account_id, tdm.rpv, tdm.pv)	
			case tda: TrackingDaspArgs => t_tracking_dasp_args_insert.format(time, tda.account_id, tda.key)	
			case uma: UMAItem if uma.adslot_id == "" => ""
			case uma: UMAItem if uma.adslot_id != "" => t_uma_skylight_insert.format(time, uma.adslot_id, uma.imp)	
			case _ =>
				""
		}
	}
}
