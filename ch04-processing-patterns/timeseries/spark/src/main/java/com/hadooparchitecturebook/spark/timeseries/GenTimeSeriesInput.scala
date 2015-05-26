package com.hadooparchitecturebook.spark.timeseries

import java.io.{OutputStreamWriter, BufferedWriter}
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by ted.malaska on 12/6/14.
 */
object GenTimeSeriesInput {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("{outputPath} {numberOfRecords} {numberOfUniqueRecords} {startTime}")
      return
    }

    val outputPath = new Path(args(0))
    val numberOfRecords = args(1).toInt
    val numberOfUniqueRecords = args(2).toInt
    val startTime = args(3).toInt

    val fileSystem = FileSystem.get(new Configuration())
    val writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outputPath)))

    val r = new Random


    for (i <- 0 until numberOfRecords) {
      val uniqueId = r.nextInt(numberOfUniqueRecords)
      val madeUpValue = r.nextInt(1000)
      val eventTime = i + startTime

      writer.write(uniqueId + "," + eventTime + "," + madeUpValue)
      writer.newLine()
    }
    writer.close()
  }
}
