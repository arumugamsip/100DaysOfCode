
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
 
 import java.util.Properties

 import org.apache.kafka.clients.producer._

object sparkstreaming {
   def main(args: Array[String]) = {
     
//Read Config file for the Auth keys
val config = ConfigFactory.load("application.conf").getConfig("twitter-conf")
val consumerKey=config.getString("consumerKey")
val consumerSecret=config.getString("consumerSecret")
val accessToken=config.getString("accessToken")
val accessTokenSecret=config.getString("accessTokenSecret")      
val tcb = new ConfigurationBuilder
val filters = "coronavirus".split(",").toSeq

 tcb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

//Loading Properties File
val url = getClass.getResource("kafka.properties")
val properties: Properties = new Properties()

 if (url != null) {
  val source = Source.fromURL(url)
  properties.load(source.bufferedReader())
}

//  Create a local StreamingContext batch interval of 5 seconds
val ssc = new StreamingContext("local[*]", "TwitterStreaming", Seconds(5))    
val auth = new OAuthAuthorization(tcb.build)  
val TOPIC="tamilboomi"
 
// Create a DStream from Twitter using our streaming context
val tweets = TwitterUtils.createStream(ssc, Some(auth),filters)   

val englishTweets = tweets.filter(_.getLang() == "en") 
val statuses = englishTweets.map(status => (status.getText(),status.getUser.getName(),status.getUser.getScreenName(),status.getCreatedAt.toString))

// Loading Properties
val props = new Properties()
val KafkaKeys =properties.keys()
while(KafkaKeys.hasMoreElements())
{
  var key =  KafkaKeys.nextElement().toString(); 
  var value = properties.getProperty(key)
  props.put(key, value)
}

statuses.foreachRDD { (rdd, time) =>
rdd.foreachPartition { partitionIter =>
val producer = new KafkaProducer[String, String](props)
partitionIter.foreach { elem =>
val dat = elem.toString()
val data = new ProducerRecord[String, String](TOPIC, null, dat)
println(dat)
  producer.send(data)
}
    producer.close()
}
}

  println("Sending Data to producer")
  

  statuses.print()
  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
 }
}