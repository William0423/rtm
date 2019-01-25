

object TestFunc {

	def main(args: Array[String]) {
		"adass|asdasdas|asd|".split("\\|").foreach(x => println(x + "12"))

		val tldParser = new DomainParser("root(5:com,jp,cn,us,tw)")


		println(tldParser.getDomainAndChannel("http://aaaw.chita.aichi.jp:8080/index.html"))
		println(tldParser.getDomainAndChannel("http://www.sina.com.cn/index.html"))
		println(tldParser.getDomainAndChannel("http://com.cn/index.html"))
		println(tldParser.getDomainAndChannel("http://aaaw.chita.aichi.jp:8080/index.html"))
		val time = System.currentTimeMillis

		/**
		  * currently take 10s to finish.
		  */ 
		for (i <- 1 to 1000000000) {
			tldParser.getDomainAndChannel("http://aaaw.chita.aichi.jp/developerworks/cn/java/j-lo-funinscala1/")
		}

		println("used time: " + (System.currentTimeMillis - time))
	}
}
