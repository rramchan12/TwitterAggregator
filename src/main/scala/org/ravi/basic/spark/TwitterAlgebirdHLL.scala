
package org.ravi.basic.spark

import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.HyperLogLog._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

object TwitterAlgebirdHLL {
  
  
  def main( args : Array[String]){
  
  System.setProperty("java.protocol.handler.pkgs", "com.sun.net.ssl.internal.www.protocol")
    System.setProperty("twitter4j.http.proxyHost", "sjc1intproxy01.crd.ge.com");
    System.setProperty("twitter4j.http.proxyPort", "8080");
    System.setProperty("twitter4j.http.useSSL", "true");
  val filter = args
  val  conf = new SparkConf().setAppName("TwitterAlgebirdHLL").setMaster("local")
  val ssc = new StreamingContext(conf, Seconds(5))
  val stream = TwitterUtils.createStream(ssc,None , filter, StorageLevel.MEMORY_ONLY_SER )
  
  val users = stream.map(status => status.getUser().getId)
  
  val hll = new HyperLogLogMonoid(10)
  //For getting totals
  var globalHll = hll.zero
  //For getting totals
  var userSet : Set[Long] = Set()
  
  val exactUsers = users.map { id => Set(id)}.reduce(_++_)
  
  exactUsers.foreachRDD(rdd => {
    val partial = rdd.first()
    userSet ++= partial
    println("Exact distinct users this batch: %d".format(partial.size))
        println("Exact distinct users overall: %d".format(userSet.size))
  })
  ssc.start()
  ssc.awaitTermination()
    
  }
  
  }
  
