package com.hadooparchitecturebook.spark.timeseries

import java.lang.Comparable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Partitioner, SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by ted.malaska on 12/6/14.
 */
object SparkTimeSeriesExecution {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("{newDataInputPath} " +
        "{outputPath} " +
        "{numberOfPartitions}")
      println("or")
      println("{newDataInputPath} " +
        "{existingTimeSeriesDataInputPath} " +
        "{outputPath} " +
        "{numberOfPartitions}")
      return
    }

    val newDataInputPath = args(0)
    val existingTimeSeriesDataInputPath = if (args.length == 4) args(1) else null
    val outputPath = args(args.length - 2)
    val numberOfPartitions = args(args.length - 1).toInt

    val sparkConf = new SparkConf().setAppName("SparkTimeSeriesExecution")
    sparkConf.set("spark.cleaner.ttl", "120000");

    val sc = new SparkContext(sparkConf)

    //Part 1 - Loading data from HDFS
    var unendedRecordsRDD = sc.hadoopFile(newDataInputPath,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      1).map(r => {
      val splits = r._2.toString.split(",")

      (new TimeDataKey(splits(0), splits(1).toLong),
        new TimeDataValue(-1, splits(2)))
    })


    var endedRecordsRDD:RDD[(TimeDataKey, TimeDataValue)] = null

    //Part 2 - get existing records if they exist
    if (existingTimeSeriesDataInputPath != null) {
      val existingDataRDD = sc.hadoopFile(existingTimeSeriesDataInputPath,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        1).map(r => {
        val splits = r._2.toString.split(",")
        (new TimeDataKey(splits(0), splits(1).toLong),
          new TimeDataValue(splits(2).toLong, splits(3)))
      })

      unendedRecordsRDD = unendedRecordsRDD
        .union(existingDataRDD.filter(r => r._2.endTime == -1))

      endedRecordsRDD = existingDataRDD.filter(r => r._2.endTime > -1)
    }

    //Part 3 - defining our partitioner
    val partitioner = new Partitioner {
      override def numPartitions: Int = numberOfPartitions

      override def getPartition(key: Any): Int = {
        Math.abs(
          key.asInstanceOf[TimeDataKey].uniqueId.hashCode() % numPartitions)
      }
    }

    val partedSortedRDD =
      new ShuffledRDD[TimeDataKey, TimeDataValue, TimeDataValue](
        unendedRecordsRDD,
        partitioner).setKeyOrdering(implicitly[Ordering[TimeDataKey]])

    //Part 4 - walk down each primaryKey to make sure the stop times are updated
    var updatedEndedRecords = partedSortedRDD.mapPartitions(it => {
      val results = new mutable.MutableList[(TimeDataKey, TimeDataValue)]

      var lastUniqueId = "foobar"
      var lastRecord: (TimeDataKey, TimeDataValue) = null

      it.foreach(r => {
        if (!r._1.uniqueId.equals(lastUniqueId)) {
          if (lastRecord != null) {
            results.+=(lastRecord)
          }
          lastUniqueId = r._1.uniqueId
          lastRecord = null
        } else {
          if (lastRecord != null) {
            lastRecord._2.endTime = r._1.startTime
            results.+=(lastRecord)
          }
        }
        lastRecord = r
      })
      if (lastRecord != null) {
        results.+=(lastRecord)
      }
      results.iterator
    })

    //Part 5 - if there was existing union them back in
    if (endedRecordsRDD != null) {
      updatedEndedRecords = updatedEndedRecords.union(endedRecordsRDD)
    }

    //Part 6 - make things pretty and save the results to HDFS
    updatedEndedRecords
      .map(r => r._1.uniqueId + "," +
      r._1.startTime + "," +
      r._2.endTime + "," +
      r._2.data)
      .saveAsTextFile(outputPath)
  }

  class TimeDataKey(val uniqueId:String, val startTime:Long) extends Serializable with Comparable[TimeDataKey] {
    override def compareTo(other:TimeDataKey): Int = {
      val compare1 = uniqueId.compareTo(other.uniqueId)
      if (compare1 == 0) {
        startTime.compareTo(other.startTime)
      } else {
        compare1
      }
    }
  }

  class TimeDataValue(var endTime:Long, val data:String) extends Serializable {}
}

