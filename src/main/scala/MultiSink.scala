package org.michael.spark.util


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.storage.StorageLevel

class MultiSink(sinks : Array[Sink], dataPersistenceLevel: Option[StorageLevel] = Some(StorageLevel.MEMORY_AND_DISK)) extends Sink {

  override def toString(): String = "MultiSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    dataPersistenceLevel match {
        case Some(persistenceLevel) => data.cache(batchId, persistenceLevel)
    }
    
    sinks.foreach(sink => sink.addBatch(data))
  }
}
