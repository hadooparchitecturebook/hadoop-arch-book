package com.hadooparchitecturebook.zombie.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object RandomFunExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
      println("RandomFunExample <masterUrl> <name> <sparkHome>")
      exit
    }

    val sc = new SparkContext(args(0), args(1), args(2), Seq("GraphXExample.jar"))

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user

    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph

    val graph = Graph(users, relationships, defaultUser)

    //The following failed I had to change the config on the following
    //worker_max_heapsize to 256MB
    //executor_total_max_heapsize to 3GB

    //This will count all the vertices
    graph.numVertices

    //This will count all the edges
    graph.numEdges

    //This will count the out going edges from each node
    val degrees = graph.outDegrees
    degrees.take(10)

    //This will count the in going edges from each node
    val degrees2 = graph.inDegrees
    degrees2.take(10)

    //This will giv eyou the node ID with the number of triangles 
    //next to that triangle
    val tr = graph.triangleCount
    tr.vertices.take(10)

    val connectedComp = graph.connectedComponents().vertices
    connectedComp.take(10)

    val graphPingBasic = graph.pregel("A", 1)(
        (id, dist, newDist) => (dist._1, dist._2 + newDist), 
        triplet => 
          Iterator.empty, 
        (a,b) => a + "|" + b)
    
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
        (a,b) => a)
        
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
        (a,b) => a)
    
    graphRooted.vertices.take(10)
     
  }
}
