package org.tanu.util

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Scan}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Row, SparkSession}

class HBSEUtil private {
  def addrow(hbaseTableName: String, batchDF: DataFrame): Unit = {

    printf("printing from hbase new")
    batchDF.rdd.foreachPartition(iter => {

      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.addResource("src/main/resources/hbase-site.xml")
      val conn = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf(hbaseTableName))

      iter.foreach(record => {
        //val i = +1
      //  val thePut = new Put(Bytes.toBytes(i))
       // thePut.add(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes(record))

        //missing part in your code
        val rowKey = java.util.UUID.randomUUID.toString
        printf(record.getString(1))
        val  put = new Put(rowKey.getBytes)
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes(record.getString(1)), Bytes.toBytes(record.getString(2)))
        table.put(put);
      })
      conn.close()
    })
  }
  /*
    batchDF.show()
    val hbasePuts= batchDF.rdd.map((row: Row) => {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.addResource("src/main/resources/hbase-site.xml")
      val conn = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf(hbaseTableName))

      val rowKey = java.util.UUID.randomUUID.toString
      val  put = new Put(rowKey.getBytes)
      printf("printing from hbase")
      println(row.getString(1))
      println(row.getString(2))
      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("id"), Bytes.toBytes(row.getString(1)))
      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("x"), Bytes.toBytes(row.getString(10)))
      //put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("eventtime"), Bytes.toBytes(row.getString(3)))
     // put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes(row.getString(4)))
      table.put(put)
      conn.close()
      printf("hbase end")
    })

    //val put = new Put(rowKey.getBytes)
    for (offset <- offsetRanges) {
      put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes(offset.partition.toString),
        Bytes.toBytes(offset.untilOffset.toString))
    }
    */
}

  object HBSEUtil {
    private val _instance = new HBSEUtil()

    def instance = _instance
  }
