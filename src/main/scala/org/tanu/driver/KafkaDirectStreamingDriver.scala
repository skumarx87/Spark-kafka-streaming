package org.tanu.driver
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.tanu.util.HBSEUtil
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.tanu.listener.TestStreamingListener
object KafkaDirectStreamingDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]")
      .setAppName("KafkaReceiver")
      .set("spark.rdd.compress","true")
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.instances", "1")
      .set("spark.executor.cores", "2")
      .set("spark.driver.port", "10027")
      .set("spark.driver.host", "192.168.0.106");
    // .set("spark.driver.bindAddress", "192.168.122.74");
    //.set("spark.ui.port", "10036");
    val sc =new SparkContext(conf)
    val ssc =new StreamingContext(sc,Seconds(5));

    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = List("Topic")
    val topicsSet = "Topic".split(",").toSet

    val kafkaParams = Map(
      "bootstrap.servers" -> "192.168.122.74:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-notes",
      "auto.offset.reset" -> "latest"
    )
  val stream = KafkaUtils.createDirectStream[String,String](
    ssc,preferredHosts,
    Subscribe[String, String](topics, kafkaParams)
  )

    stream.foreachRDD((rdd, batchTime) => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(
      offset =>println(offset.topic +":"+offset.fromOffset +":"+offset.untilOffset)
      )
      //println(offsetRanges)
    //rdd.collect().foreach(println)
    })
    //stream.map(record => (record.key, record.value))
    stream.map(record=>(record.value().toString)).print
    ssc.addStreamingListener(new TestStreamingListener())
    ssc.start()
    ssc.awaitTermination()

  }

}
