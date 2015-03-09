package com.hadooparchitecturebook.spark.peaksandvalleys

import java.io.{OutputStreamWriter, BufferedWriter}
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Created by ted.malaska on 12/7/14.
 */
object GenPeaksAndValleys {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("{outputPath} {numberOfRecords} {numberOfUniqueIds}")
      return
    }

    val outputPath = new Path(args(0))
    val numberOfRecords = args(1).toInt
    val numberOfUniqueIds = args(2).toInt

    val fileSystem = FileSystem.get(new Configuration())

    val writer = new BufferedWriter( new OutputStreamWriter(fileSystem.create(outputPath)))

    val r = new Random()

    var direction = 1
    var directionCounter = r.nextInt(numberOfUniqueIds * 10)
    var currentPointer = 0

    for (i <- 0 until numberOfRecords) {
      val uniqueId = r.nextInt(numberOfUniqueIds)

      currentPointer = currentPointer + direction
      directionCounter = directionCounter - 1
      if (directionCounter == 0) {
        directionCounter = r.nextInt(numberOfUniqueIds * 10)
        direction = direction * -1
      }

      writer.write(uniqueId + "," + i + "," + currentPointer)
      writer.newLine()
    }
    writer.close()
  }
}
