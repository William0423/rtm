
import kafka.producer._

class KafkaProducerPool[K, V](brokers: String) {

    val queue = new scala.collection.mutable.SynchronizedQueue[Producer[K, V]]
    
    def createKafkaProducer: Producer[K, V] = {
        val props = new java.util.Properties()
        props.put("metadata.broker.list", brokers)
        props.put("serializer.class", "kafka.serializer.StringEncoder")

        val config = new ProducerConfig(props)
        new Producer[K, V](config)
    }
    
    def borrow(): Producer[K, V] = {
        try {
            queue.dequeue()
        } catch {
            case _ : NoSuchElementException => createKafkaProducer
        }
    }

    def returnProducer(p: Producer[K, V]) {
        queue.enqueue(p)
    }
}
