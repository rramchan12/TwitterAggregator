package org.ravi.basic.spark

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils

object Twitter4jTest {
  def main(args: Array[String]) {
    val filters =  Array("Modi")
 
    System.setProperty("twitter4j.oauth.consumerKey", "Gsxftie3p0zbk90vDEDK9KBXu")
    System.setProperty("twitter4j.oauth.consumerSecret", "vVHxZxtaFFBhfgVgZk9K7S0o2bfLjxZYRap7kc2HDE79jC8gqg")
    System.setProperty("twitter4j.oauth.accessToken", "28599225-f41C4vLTdGrJ0CyG1zaq9yfdKskSdx7zyO2EpMm12")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "XaoEF3huO6kb0n6pRajYOyyj1sfvL0ti5dSDrZfwZLcd7")
    System.setProperty("twitter4j.http.proxyHost", "sjc1intproxy01.crd.ge.com");
    System.setProperty("twitter4j.http.proxyPort", "8080");
    System.setProperty("twitter4j.http.useSSL", "true");
 
    val conf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local")
 
    val ssc = new StreamingContext(conf, Seconds(60))
    val stream = TwitterUtils.createStream(ssc, None, filters)
   
    stream.foreach(x => println(x))
    
    ssc.start();
    
    ssc.awaitTermination();
 
    ssc.start()
    ssc.awaitTermination()
  }
}