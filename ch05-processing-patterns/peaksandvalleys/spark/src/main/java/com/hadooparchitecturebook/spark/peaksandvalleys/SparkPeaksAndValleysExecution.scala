package com.hadooparchitecturebook.spark.peaksandvalleys

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Partitioner, SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by ted.malaska on 12/7/14.
 */
object SparkPeaksAndValleysExecution {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("{inputPath} {outputPath} {numberOfPartitions}")
      return
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val numberOfPartitions = args(2).toInt

    val sparkConf = new SparkConf().setAppName("SparkTimeSeriesExecution")
    sparkConf.set("spark.cleaner.ttl", "120000");

    val sc = new SparkContext(sparkConf)

    //Part 1 : Reading in the data
    var originalDataRDD = sc.hadoopFile(inputPath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      1).map(r => {
      val splits = r._2.toString.split(",")
      (new DataKey(splits(0), splits(1).toLong), splits(2).toInt)
    })

    //Part 2 : Partition to partition by primaryKey only
    val partitioner = new Partitioner {
      override def numPartitions: Int = numberOfPartitions

      override def getPartition(key: Any): Int = {
        Math.abs(key.asInstanceOf[DataKey].uniqueId.hashCode() % numPartitions)
      }
    }

    //Part 3 : Partition and sort
    val partedSortedRDD =
      new ShuffledRDD[DataKey, Int, Int](
        originalDataRDD,
        partitioner).setKeyOrdering(implicitly[Ordering[DataKey]])

    //Part 4 // MapPartition to do windowing
    val pivotPointRDD = partedSortedRDD.mapPartitions(it => {

      val results = new mutable.MutableList[PivotPoint]

      //Part 5 // Keeping context
      var lastUniqueId = "foobar"
      var lastRecord: (DataKey, Int) = null
      var lastLastRecord: (DataKey, Int) = null

      var position = 0

      it.foreach( r => {
        position = position + 1

        if (!lastUniqueId.equals(r._1.uniqueId)) {

          lastRecord = null
          lastLastRecord = null
        }

        //Part 6 : Finding those peaks and valleys
        if (lastRecord != null && lastLastRecord != null) {
          if (lastRecord._2 < r._2 && lastRecord._2 < lastLastRecord._2) {
            results.+=(new PivotPoint(r._1.uniqueId,
              position,
              lastRecord._1.eventTime,
              lastRecord._2,
              false))
          } else if (lastRecord._2 > r._2 && lastRecord._2 > lastLastRecord._2) {
            results.+=(new PivotPoint(r._1.uniqueId,
              position,
              lastRecord._1.eventTime,
              lastRecord._2,
              true))
          }
        }
        lastUniqueId = r._1.uniqueId
        lastLastRecord = lastRecord
        lastRecord = r

      })

      results.iterator
    })

    //Part 7 : pretty everything up
    pivotPointRDD.map(r => {
      val pivotType = if (r.isPeak) "peak" else "valley"
      r.uniqueId + "," +
        r.position + "," +
        r.eventTime + "," +
        r.eventValue + "," +
        pivotType
    } ).saveAsTextFile(outputPath)

  }


  class DataKey(val uniqueId:String, val eventTime:Long)
    extends Serializable with Comparable[DataKey] {
    override def compareTo(other:DataKey): Int = {
      val compare1 = uniqueId.compareTo(other.uniqueId)
      if (compare1 == 0) {
        eventTime.compareTo(other.eventTime)
      } else {
        compare1
      }
    }
  }

  class PivotPoint(val uniqueId: String,
                   val position:Int,
                   val eventTime:Long,
                   val eventValue:Int,
                   val isPeak:Boolean) extends Serializable {}

}
