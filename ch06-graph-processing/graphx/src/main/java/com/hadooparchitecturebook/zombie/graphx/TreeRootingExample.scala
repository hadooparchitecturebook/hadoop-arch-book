package com.hadooparchitecturebook.zombie.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object TreeRootingExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
      println("TreeRooterExample <masterUrl> <name> <sparkHome>")
      exit
    }

    val sc = new SparkContext(args(0), args(1), args(2), Seq("GraphXExample.jar"))

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
          (1L, ("Foo", "X")),
          (2L, ("Foo", "X")),
          (3L, ("Foo", "X")),
          (4L, ("Foo", "X")),
          (5L, ("Foo", "X")),
          (6L, ("Foo", "X")),
          (7L, ("Foo", "X")),
          (8L, ("Foo", "X")),
          (9L, ("Foo", "X")),
          (10L, ("Foo", "X")),
          (11L, ("Foo", "X")),
          (12L, ("Foo", "X")),
          (13L, ("Foo", "X")),
          (14L, ("Foo", "X")),
          (15L, ("Foo", "X")),
          (16L, ("Foo", "X")),
          (17L, ("Foo", "X")),
          (18L, ("Foo", "X")),
          (19L, ("Foo", "X")),
          (20L, ("Foo", "X"))
          ))
    // Create an RDD for edges

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
          Edge(10L, 9L, "X"),
          Edge(10L, 8L, "X"),
          Edge(8L, 7L, "X"),
          Edge(8L, 5L, "X"),
          Edge(5L, 4L, "X"),
          Edge(5L, 3L, "X"),
          Edge(5L, 2L, "X"),
          Edge(9L, 6L, "X"),
          Edge(6L, 1L, "X"),
          Edge(20L, 11L, "X"),
          Edge(20L, 12L, "X"),
          Edge(20L, 13L, "X"),
          Edge(20L, 14L, "X"),
          Edge(11L, 15L, "X"),
          Edge(20L, 16L, "X"),
          Edge(14L, 17L, "X"),
          Edge(20L, 18L, "X"),
          Edge(17L, 19L, "X")
          ))
    // Define a default user in case there are relationship with missing user

    val defaultUser = ("FooBar", "Missing")
    // Build the initial Graph

    val graph = Graph(users, relationships, defaultUser)

    val graphPing = graph.pregel("A", 1)(
      (id, dist, message) => {
        if (!message.equals("A")) {
          (dist._1, dist._2 + "_" + message)
        } else {
          (dist._1, dist._2)
        }
      },
      triplet =>
        Iterator((triplet.dstId, "F")),
      (a, b) => a)

    graphPing.vertices.take(10)

    val graphRooted = graph.pregel("A")(
      (id, dist, message) => {
        if (!message.equals("A")) {
          (dist._1, dist._2 + "_" + message)
        } else {
          (dist._1, dist._2)
        }
      }, triplet => {
        var indexOfUnderScore = triplet.srcAttr._2.indexOf("_")
        if (indexOfUnderScore == -1) {
          Iterator((triplet.dstId, triplet.srcId.toString))
        } else {
          Iterator((triplet.dstId, triplet.srcAttr._2.substring(indexOfUnderScore + 1)))
        }
      },
      (a, b) => a)

    graphRooted.vertices.take(10)
  }
}
