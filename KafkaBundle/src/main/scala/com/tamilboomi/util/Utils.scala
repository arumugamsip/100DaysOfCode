package com.tamilboomi.util

import java.util.Properties
import org.apache.kafka.clients.producer._
import java.io.FileReader

object Utils {
    
    def buildProperties(configFileName: String): Properties = {
    val properties: Properties = new Properties
    properties.load(new FileReader(configFileName))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties
  }
    
  def geturl(url: String):String=scala.io.Source.fromURL(url).mkString
}