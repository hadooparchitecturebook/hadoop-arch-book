package com.hadooparchitecturebook.zombie.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object ZombeBiteGraph {
def main(args: Array[String]) {
    if (args.length == 0) {
      println("ZombeBiteGraph <masterUrl> <name> <sparkHome>")
      exit
    }

    val sc = new SparkContext(args(0), args(1), args(2), Seq("GraphXExample.jar"))

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String))] =
      sc.parallelize(Array(
          (1L, ("Human")),
          (2L, ("Human")),
          (3L, ("Human")),
          (4L, ("Human")),
          (5L, ("Human")),
          (6L, ("Zombe")),
          (7L, ("Human")),
          (8L, ("Human")),
          (9L, ("Human")),
          (10L, ("Human")),
          (11L, ("Human")),
          (12L, ("Human")),
          (13L, ("Human")),
          (14L, ("Human")),
          (15L, ("Human")),
          (16L, ("Zombe")),
          (17L, ("Human")),
          (18L, ("Human")),
          (19L, ("Human")),
          (20L, ("Human")),
          (21L, ("Human")),
          (22L, ("Human")),
          (23L, ("Human")),
          (24L, ("Human")),
          (25L, ("Human"))
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
          Edge(5L, 13L, "X"),
          Edge(20L, 14L, "X"),
          Edge(11L, 15L, "X"),
          Edge(20L, 16L, "X"),
          Edge(14L, 17L, "X"),
          Edge(1L, 17L, "X"),
          Edge(20L, 18L, "X"),
          Edge(21L, 18L, "X"),
          Edge(21L, 22L, "X"),
          Edge(4L, 23L, "X"),
          Edge(25L, 15L, "X"),
          Edge(24L, 3L, "X"),
          Edge(21L, 19L, "X")
          ))
    // Define a default user in case there are relationship with missing user

    val defaultUser = ("Rock")
    // Build the initial Graph

    val graph = Graph(users, relationships, defaultUser)

    graph.triangleCount
    
    val graphBites = graph.pregel(0L)(
      (id, dist, message) => {
        if (dist.equals("Zombe")) {
          (dist + "_" + message)
        } else if (message != 0){
          "Zombe" + "_" + message
        } else {
          dist + "|" + message
        }
      }, triplet => {
        if (triplet.srcAttr.startsWith("Zombe") && triplet.dstAttr.startsWith("Human")) {
          var stringBitStep = triplet.srcAttr.substring(triplet.srcAttr.indexOf("_") + 1)
          var lastBitStep = stringBitStep.toLong
          Iterator((triplet.dstId, lastBitStep + 1))
        } else if (triplet.srcAttr.startsWith("Human") && triplet.dstAttr.startsWith("Zombe")) {
          var stringBitStep = triplet.dstAttr.substring(triplet.dstAttr.indexOf("_") + 1)
          var lastBitStep = stringBitStep.toLong
          Iterator((triplet.srcId, lastBitStep + 1))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(b, a))

    graphBites.vertices.take(30)
  }
}
