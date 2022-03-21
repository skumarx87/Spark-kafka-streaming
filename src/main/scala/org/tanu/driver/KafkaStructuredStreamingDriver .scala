package org.tanu.driver

/**
 * @author ${user.name}
 */
import scala.math.random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.tanu.listener.StreamingListener

import java.util.Properties
import org.apache.spark.sql.streaming.Trigger

import java.time.Instant
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import org.apache.spark.sql.functions._
import org.tanu.util.HBSEUtil
import org.tanu.listener.TestStreamingListener

object KafkaStructuredStreamingDriver {

  def main(args: Array[String]): Unit = {
  /*
    val spark: SparkSession = SparkSession.builder()
      .appName("MY-APP")
      .getOrCreate()
    */
    val hbasetable = "kafkahbase1"
    val AppName = "Kafka-Offset-Management-Blog"
    val conf = new SparkConf().setAppName(AppName)
      .set("spark.driver.port", "10027")
      .set("spark.driver.host", "192.168.0.106")
      .set("spark.ui.port", "10036")
     // .setMaster("spark://192.168.122.74:7077")
      .setMaster("local[1]")//Uncomment this line to test while developing on a workstation
    val sc = new SparkContext(conf)
    implicit val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val StreamingListener = new StreamingListener(AppName);
    StreamingListener.collectStreamsMetrics

    /*
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()
*/
    import spark.sqlContext.implicits._

    spark.catalog.clearCache()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    spark.sparkContext.setLogLevel("ERROR")
    //spark.sparkContext.setCheckpointDir("hdfs:///user/sathish/checkpoints")
    spark.sparkContext.setCheckpointDir("/user/sathish/checkpoints")


    System.gc()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.122.74:9092")
      .option("subscribe", "Topic")
      .option("kafka.group.id","MYID")
      .option("startingoffsets", "latest")
      .load()

    df.printSchema()


    val schema = new StructType()
      .add("id", StringType)
      .add("x", StringType)
      .add("eventtime", StringType)
      .add("name", StringType)

    // parse csv stream
    //112,sathish,ramya,laksha

    val csvservice = df.select(col("value").cast("string")) .alias("csv").select("csv.*")
    val monitoring_df = csvservice
      .selectExpr("split(value,',')[0] as customer_id"
            ,"split(value,',')[1] as x"
            ,"split(value,',')[2] as eventtime"
            ,"split(value,',')[3] as name"
      )
    /*
    // parse Json Stream
    //{"id":"123","x":"sathish","eventtime":"Sasasas","name":"new1"}

    val idservice = df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("jsondata"))
      .select("jsondata.*")


    val monitoring_df = idservice
      .selectExpr("cast(id as string) id",
        "cast(x as string) x",
        "cast(eventtime as string) eventtime",
        "cast(name as string) name")
*/
    val monitoring_stream = monitoring_df.writeStream
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if(!batchDF.isEmpty)
        {
          batchDF.persist()
          printf("At %d, the %dth microbatch has %d records and %d partitions \n", Instant.now.getEpochSecond, batchId, batchDF.count(), batchDF.rdd.partitions.size)
          printf("calling hbase")
          HBSEUtil.instance.addrow(hbasetable,batchDF)
          batchDF.write.mode(SaveMode.Overwrite).option("path", "/home/ec2-user/environment/spark/spark-local/tmp").saveAsTable("mytable")
          spark.catalog.refreshTable("mytable")
          batchDF.show()
          batchDF.unpersist()
          spark.catalog.clearCache()
        }
      }
      .start()
      .awaitTermination()
  }

}