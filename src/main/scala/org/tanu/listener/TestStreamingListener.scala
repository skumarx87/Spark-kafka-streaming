package org.tanu.listener
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted

class TestStreamingListener extends org.apache.spark.streaming.scheduler.StreamingListener  {
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    println("Receiver Started: " + receiverStarted.receiverInfo.name )
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    println("Receiver Error: " + receiverError.receiverInfo.lastError)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    println("Receiver Stopped: " + receiverStopped.receiverInfo.name)
    println("Reason: " + receiverStopped.receiverInfo.lastError + " : " + receiverStopped.receiverInfo.lastErrorMessage)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted){
    println("Batch started with " + batchStarted.batchInfo.numRecords + " records")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted){
    println("Batch completed with " + batchCompleted.batchInfo.numRecords + " records");
    println("Batch completed with " + batchCompleted.batchInfo.streamIdToInputInfo + " records")
    batchCompleted.batchInfo.streamIdToInputInfo.foreach(topic => {
      val partitionOffsets = topic._2.metadata("offsets").asInstanceOf[List[OffsetRange]]
    })
    /*
    batchCompleted.batchInfo.streamIdToInputInfo.foreach {
      case(movie, rating) => println(s"key: $movie, value: $rating")
        println ("sasa")

    }
    */
  }

}
