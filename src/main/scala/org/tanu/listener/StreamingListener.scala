package org.tanu.listener


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * @author dtaieb
 */
class StreamingListener(val appName: String)(implicit spark: SparkSession)  {
  val metricsTag = s"spark_app:$appName"
  println("@@@@@@@@@@  calling Listener class @@@@@@@@@@@@@@@@" )
  def collectStreamsMetrics(): Unit = {
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress = event.progress
        val queryNameTag = s"query_name:${progress.name}"
        println("streaming.batch_id", progress.batchId, metricsTag, queryNameTag)
        println("streaming.input_rows", progress.numInputRows, metricsTag, queryNameTag)
        println("streaming.input_rows_per_sec", progress.inputRowsPerSecond, metricsTag, queryNameTag)
        println("streaming.process_rows_per_sec", progress.processedRowsPerSecond, metricsTag, queryNameTag)
        println("Starting offset:" + progress.sources(0).startOffset)
        println("Ending offset:" + progress.sources(0).endOffset)
      }
    })
  }
}