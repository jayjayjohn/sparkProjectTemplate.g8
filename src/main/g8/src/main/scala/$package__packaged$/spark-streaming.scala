package com.example.sparkProject

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import com.typesafe.config.ConfigFactory
import java.sql.Timestamp

object stream_processing_app {
  def main(args: Array[String]): Unit = {
    println("Stream Processing Application Started ...")
	
	val conf = ConfigFactory.load.getConfig(args(0))
	
	val kafka_topic_name = conf.getString("bootstrap.kafka.topic")
    val kafka_bootstrap_servers = conf.getString("bootstrap.servers")
	
	 val spark = SparkSession.
      builder().
      master(conf.getString("execution.mode")).
      appName("Get Meet Up Stream").
      getOrCreate()
	  
	
	import spark.implicits._
	
	spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
	
	
	val lines = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", kafka_bootstrap_servers).
      option("subscribe", kafka_topic_name).
      option("includeTimestamp", true).
      load.
      selectExpr("CAST(value AS STRING)", "timestamp").
      as[(String, Timestamp)]
	  
	  
	  
	  
	val streamWriter = lines.
      writeStream.
      outputMode("update").
      format("console").
      trigger(Trigger.ProcessingTime("1 seconds"))
	// Trigger.ProcessingTime(0) will return result asap
	
	
	val query = streamWriter.start()   
	query.awaitTermination()

}
}
