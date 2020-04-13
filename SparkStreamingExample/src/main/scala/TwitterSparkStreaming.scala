
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
import org.apache.log4j.Level
import com.typesafe.config.{ Config, ConfigFactory }

object sparkstreaming {
   def main(args: Array[String]) = {

//Read Config file for the Auth keys
val config = ConfigFactory.load("application.conf").getConfig("twitter-conf")
val consumerKey=config.getString("consumerKey")
val consumerSecret=config.getString("consumerSecret")
val accessToken=config.getString("accessToken")
val accessTokenSecret=config.getString("accessTokenSecret")      
val tcb = new ConfigurationBuilder
val filters = "bigdata".split(",").toSeq

 tcb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

// // Create a local StreamingContext batch interval of 5 seconds
val ssc = new StreamingContext("local[*]", "TwitterStreaming", Seconds(5))    
val auth = new OAuthAuthorization(tcb.build)  

// Create a DStream from Twitter using our streaming context
val tweets = TwitterUtils.createStream(ssc, Some(auth),filters)    
val statuses = tweets.map(status => status.getText())
 
statuses.print()
ssc.start()             // Start the computation
ssc.awaitTermination()  // Wait for the computation to terminate
   }
}