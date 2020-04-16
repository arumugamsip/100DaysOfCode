import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkStreaming_Consumer {
     def main(args: Array[String]) = {
     
  
  val ssc = new StreamingContext("local[*]", "StreamingKafkaConsumer", Seconds(2))
  
  val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "group1",
  "auto.offset.reset" -> "latest"
)

val topics = Array( "tamilboomi")
val stream = KafkaUtils.createDirectStream[String, String](
  ssc,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)


//stream.map(record => (record.key, record.value))
  stream.foreachRDD { rdd =>
 rdd.foreach{ data =>
    println(data.value())
 }
 }
  
  
  
  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
  
}
}