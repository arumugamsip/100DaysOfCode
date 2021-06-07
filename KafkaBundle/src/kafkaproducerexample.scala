import java.util.{ Date, Properties }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import scala.util.Random
import scala.Range

object kafkaproducerexample  {
   def main(args: Array[String]) = {
/*  val events = args(0).toInt
  val topic = args(1)
  val brokers = args(2)*/
    val events = 10
  val topic = "tamilboomi"
  val brokers = "192.168.56.102:9092"
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

 

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  for (nEvents <- Range(0, events)) {
    val runtime = new Date().getTime()
    val ip = "192.168.56.102";
    val msg = runtime + "," + nEvents + ",www.tamilboomi.com," + ip
    val data = new ProducerRecord[String, String](topic, ip, msg)
    producer.send(data)
  }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
   }
}