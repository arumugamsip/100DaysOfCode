

package com.tamilboomi.main

object test extends App {
  val response=geturl("http://api.openweathermap.org/data/2.5/forecast?id=524901&zip=600113,IN&cnt=1&appid=c854fe1b651d9266650071e13828e7c2")
  println(response)
  def geturl(url: String):String=scala.io.Source.fromURL(url).mkString
}