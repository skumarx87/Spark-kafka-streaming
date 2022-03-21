package org.tanu.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

import java.time.Instant

object sathishstreaming {
  def main(args: Array[String]): Unit = {
    val consumerGroupID = "spark-streaming-notes"
    val hbaseTableName = "stream_kafka_offsets"
    val zkQuorum = "192.168.122.74:2181,192.168.122.121:2181"
    val topic = "Topic"

    val schema = new StructType()
      .add("id", StringType)
      .add("x", StringType)
      .add("eventtime", StringType)
      .add("name", StringType)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.122.74:9092")
      .option("subscribe", "sssssss")
      .option("startingOffsets", "latest")
      //.option("endingOffsets", "latest")
      .load()
      //.select(col("value").cast("string"))
    df.printSchema()
/*
    val wordCount = df
      .select(explode(split(col("value"), ",")).alias("wordss"))
*/
val wordCount = df.selectExpr("CAST(value AS STRING)")
  .select(from_json(col("value"), schema).as("data"))
  .select("data.*")
    val monitoring_df = wordCount
      .selectExpr("cast(id as string) id",
        "cast(x as string) x",
        "cast(eventtime as string) eventtime",
        "cast(name as string) name")
    val monitoring_stream = monitoring_df.writeStream
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if(!batchDF.isEmpty)
        {
          batchDF.persist()
          printf("At %d, the %dth microbatch has %d records and %d partitions \n", Instant.now.getEpochSecond, batchId, batchDF.count(), batchDF.rdd.partitions.size)
          batchDF.show()

          batchDF.write.mode(SaveMode.Overwrite).option("path", "/home/ec2-user/environment/spark/spark-local/tmp").saveAsTable("mytable")
          spark.catalog.refreshTable("mytable")

          batchDF.unpersist()
          spark.catalog.clearCache()
        }
      }
      .start()
      .awaitTermination()

  }
}
