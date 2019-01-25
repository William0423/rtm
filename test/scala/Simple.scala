

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Simple {

	def output[T](decode: T => Int) {
	}

	def compute[K, V](map: Array[Byte] => (K, V),
		decoder: (K, V) => Int) {
	//	output(decoder)
	}

	def main(args: Array[String]) {
		val file = "/tmp/README.md"
		val conf = new SparkConf().setAppName("Simple Spark Example")

		println("")

		val sc = new SparkContext(conf)
		val data = sc.textFile(file, 2)
		val words = data.flatMap(line => line.split(","))
		val word_counts = words.map(word => (word, 1)).reduceByKey((a, b) => a+b)
		word_counts.foreach(wc => println(wc))
	}
	
}