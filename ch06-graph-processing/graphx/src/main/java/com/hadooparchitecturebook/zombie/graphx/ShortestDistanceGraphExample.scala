package com.hadooparchitecturebook.zombie.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

class ShortestDistanceGraphExample {
def main(args: Array[String]) {
    if (args.length == 0) {
      println("ShortestDistanceGraphExample <masterUrl> <name> <sparkHome>")
      exit
    }

    val sc = new SparkContext(args(0), args(1), args(2), Seq("GraphXExample.jar"))

    class Msg(val isFirst: Boolean, val sourceVertixId: Int, val distVertixId: Int, val cost: Int, val path: String) {}
  
    class Node(var isSource: Boolean, var isDist: Boolean, var cost: Int, var path: String ) {}
    
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (Node))] =
      sc.parallelize(Array(
          (1L, (new Node(false, false, Int.MaxValue, ""))),
          (2L, (new Node(false, false, Int.MaxValue, ""))),
          (3L, (new Node(false, false, Int.MaxValue, ""))),
          (4L, (new Node(false, false, Int.MaxValue, ""))),
          (5L, (new Node(false, false, Int.MaxValue, ""))),
          (6L, (new Node(false, false, Int.MaxValue, ""))),
          (7L, (new Node(false, false, Int.MaxValue, ""))),
          (8L, (new Node(false, false, Int.MaxValue, ""))),
          (9L, (new Node(false, false, Int.MaxValue, ""))),
          (10L, (new Node(false, false, Int.MaxValue, ""))),
          (11L, (new Node(false, false, Int.MaxValue, ""))),
          (12L, (new Node(false, false, Int.MaxValue, ""))),
          (13L, (new Node(false, false, Int.MaxValue, ""))),
          (14L, (new Node(false, false, Int.MaxValue, ""))),
          (15L, (new Node(false, false, Int.MaxValue, ""))),
          (16L, (new Node(false, false, Int.MaxValue, ""))),
          (17L, (new Node(false, false, Int.MaxValue, ""))),
          (18L, (new Node(false, false, Int.MaxValue, ""))),
          (19L, (new Node(false, false, Int.MaxValue, ""))),
          (20L, (new Node(false, false, Int.MaxValue, ""))),
          (21L, (new Node(false, false, Int.MaxValue, ""))),
          (22L, (new Node(false, false, Int.MaxValue, ""))),
          (23L, (new Node(false, false, Int.MaxValue, ""))),
          (24L, (new Node(false, false, Int.MaxValue, ""))),
          (25L, (new Node(false, false, Int.MaxValue, "")))
          ))
    // Create an RDD for edges

    val relationships: RDD[Edge[Int]] =
      sc.parallelize(Array(
          Edge(10L, 9L, 1),
          Edge(10L, 8L, 4),
          Edge(8L, 7L, 7),
          Edge(8L, 5L, 1),
          Edge(5L, 4L, 3),
          Edge(5L, 3L, 8),
          Edge(5L, 2L, 4),
          Edge(9L, 6L, 6),
          Edge(6L, 1L, 3),
          Edge(20L, 11L, 2),
          Edge(20L, 12L, 5),
          Edge(20L, 13L, 7),
          Edge(5L, 13L, 9),
          Edge(20L, 14L, 8),
          Edge(11L, 15L, 4),
          Edge(20L, 16L, 3),
          Edge(14L, 17L, 3),
          Edge(1L, 17L, 9),
          Edge(20L, 18L, 3),
          Edge(21L, 18L, 5),
          Edge(21L, 22L, 6),
          Edge(4L, 23L, 1),
          Edge(25L, 15L, 2),
          Edge(24L, 3L, 3),
          Edge(21L, 19L, 4)
          ))
    // Define a default user in case there are relationship with missing user

    
          
    val defaultUser = new Node(false, false, 0, "")
    // Build the initial Graph

    val graph = Graph(users, relationships, defaultUser)

    var initialMsg = new Msg(true, 16, 6, 0, "")
    
    val graphBites = graph.pregel(initialMsg)(
      (id, dist, message) => {
        
        if (message.isFirst) {
          if (id.equals(message.sourceVertixId)) {
            dist.cost = 0
            dist.path = message.distVertixId.toString
            dist
          } else if (id.equals(message.distVertixId)) {
            dist.isDist = true
            dist.path = ""
            dist
          } 
          dist
        } else  {
          dist.cost = message.cost
          dist.path = "|" + message.path
          dist
        }
      }, triplet => {
        if (triplet.srcAttr.isSource) {
          var newMsg = new Msg(false, -1, triplet.srcAttr.path.toInt, triplet.srcAttr.cost + triplet.attr, triplet.srcAttr.path)
          Iterator((triplet.dstId, newMsg))
        } else if (!triplet.srcAttr.isDist && triplet.dstAttr.cost > triplet.srcAttr.cost + triplet.attr){
          val finalDistId = triplet.srcAttr.path.substring(triplet.srcAttr.path.lastIndexOf("_") + 1).toInt
          var newMsg = new Msg(false, -1, finalDistId, triplet.srcAttr.cost + triplet.attr, triplet.srcAttr.path)
          Iterator((triplet.dstId, newMsg))
        } else {
          Iterator.empty
        }
      },
      (a, b) => if (a.cost < b.cost) { a } else { b })

    graphBites.vertices.take(30)
  }

  
  
}
