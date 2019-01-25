
import kafka.producer._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import zampdata.records.pb.MegatronLog._

object TestKafkaPool extends KafkaProducerPool[String, String]("localhost:9092")

object StreamingExample {

    def firstChar(record: String): (String, Int) = {
        (record.substring(0, 1), 1)
    }

/*
    def parseMegatron(record: Array[Byte]) = {
        val megatron = Megatornlogs.parseFrom(record);

        val results = new scala.collection.mutable.MutableList[(String, (Int, Int))]

        def emit(r: (String, (Int, Int))) { results += r }

        for (log <- megatron.getLogsList) {
            val vendor_id = log.getVendorId
            val page_url = log.getPageUrl

            /**
                std::string host;
                std::string domain;
                std::string channel;
                
                if (page_url.compare(0, 4, HTTP_PREFIX) != 0) {
                  // 当作是mobile平台
                  domain = channel = page_url;
                } else {
                  extractor_.GetHost(page_url, &host);
                  extractor_.GetChannelAndDomain(host, &channel, &domain);
                  if (domain.empty()) {
                    domain = channel = "N/A";
                  } 
            */
            val (domain, channel) = DomainParser.getDomainAndChannel(page_url)

            for (had <- log.getHandledAdsList) {
                val total_error_code = if (had.hasTotalErrorCode) had.getTotalErrorCode else -1
                val tec_str = "E%03d".format(total_error_code)
                val vendor_ec_key = "A" + vendor_id + "|" + tec_str;
            /*
                  std::string vendor_domain_key = zmt::SliceConcat(vendor_key, "|", domain);
                  UpdateItem(all_error_codes_, vendor_ec, 1);
                  if (0 == total_error_code) {
                    UpdateItem(publisher_, vendor_domain_key, 1, 1);
                    UpdateItem(vendor_, vendor_key, 1, 1);
                  } else {
                    UpdateItem(publisher_, vendor_domain_key, 1, 0);
                    UpdateItem(vendor_, vendor_key, 1, 0);
                  }
            */
                val vendor_domain_key = "P" + vendor_id + "|" + domain
                emit(("A" + vendor_ec_key, (1, 0)))
                val vendor_key = "V" + vendor_id.toString
                if (total_error_code == 0) {
                    emit((vendor_key, (1, 1)))
                    emit((vendor_domain_key, (1, 1)))
                } else {
                    emit((vendor_key, (1, 0)))
                    emit((vendor_domain_key, (1, 0)))
                }

                for (bid <- had.getBidCodeList) {
                    val cap_adgroup_id = if (bid.hasAdgroupId) bid.getAdgroupId else 0
                    val cap_error_code = if (bid.hasErrorCode) bid.getErrorCode else -1

                    val ec_str = "E%03d".format(cap_error_code)

                    val ec_key = "E" + vendor_id + "|" + cap_adgroup_id + "|" + ec_str;
                    val adgroup_id_key = "B" + vendor_id + "|" + cap_adgroup_id;

                    emit((ec_key, (1, 0)))
                    emit((adgroup_id_key, (1, 0)))
                    if (cap_error_code == 0) emit((adgroup_id_key, (1, 0)))
                }
            }
        }
        results
    }
*/

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Streaming")
        val ssc = new StreamingContext(conf, Seconds(2))

//      socket text stream
//      val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

        val lines = KafkaUtils.createStream(ssc, "localhost:2181",
            "kafkatest", Map("tests" -> 1), StorageLevel.MEMORY_AND_DISK_SER)

        val words = lines.flatMap(_._2.split(" "))
        val pairs = words.map(firstChar)
        
        val wordCounts = pairs.reduceByKeyAndWindow(
            (a: Int, b: Int) => a+b, 
//            (a:Int, b:Int)=> a-b,
            Seconds(30),
            Seconds(2))

        val megatron = Megatornlogs.parseFrom(new Array[Byte](0));
//      val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((a : Int, b : Int) => a+b, Seconds(10), Seconds(2))
//      wordCounts.print()

//      val producerPool = new KafkaProducerPool[String, String]("localhost:9094")

        wordCounts.foreachRDD { rdd =>
            rdd.foreachPartition { partitionOfRecords =>
                val producer = TestKafkaPool.borrow()
                partitionOfRecords.foreach { record =>
                    producer.send(new KeyedMessage[String, String](
                        "testResult", record.toString()))
                }
                TestKafkaPool.returnProducer(producer)
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}
