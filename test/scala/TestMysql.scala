import java.sql.DriverManager

object TestMysql {

    def main(args: Array[String]) {
        //classOf[com.mysql.jdbc.Driver]
        val url = "jdbc:mysql://localhost:3306/rtm"
        val conn = DriverManager.getConnection(url, "user_rtm", "rtmpass")

        val sql = "INSERT INTO `t_rtb_bidding` " +
		"    (data_time, vendor_id, adgroup_id, capt, bid, partial, update_time) " +
		"VALUES ('2015-01-20 18:08:00', %s, 0, 1, 1, 0, '2015-01-20 18:08:00')"

		val time = System.currentTimeMillis()
		val statement = conn.createStatement()
		for (i <- 1 to 10000) {
			statement.addBatch(sql.format(i))
		}
		statement.executeBatch()

		println(System.currentTimeMillis - time)
    }
}
