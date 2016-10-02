import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object graphfrauddetection {

  def delta(u: Array[VertexId], v: VertexId): Double = { if (u contains v) 1.0 else 0.0 }

  def runWithOptions2[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED],
    numIter: Int,
    resetProb: Double = 0.15,
    srcIds: Array[VertexId] //,
    //srcId: Option[VertexId] = None
    ): Graph[Double, Double] =
    {
      val personalized = true

      // Initialize the PageRank graph with each edge attribute having
      // weight 1/outDegree and each vertex with attribute resetProb.
      // When running personalized pagerank, only the source vertex
      // has an attribute resetProb. All others are set to 0.

      var rankGraph: Graph[Double, Double] = graph
        // Associate the degree with each vertex
        .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
        // Set the weight on the edges based on the degree
        .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
        // Set the vertex attributes to the initial pagerank values
        .mapVertices { (id, attr) =>
          if (!(!(srcIds contains id) && personalized)) resetProb else 0.0
        }

      var iteration = 0
      var prevRankGraph: Graph[Double, Double] = null
      while (iteration < numIter) {
        rankGraph.cache()
        // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
        // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
        val rankUpdates = rankGraph.aggregateMessages[Double](
          ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)
        // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
        // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
        // edge partitions.
        prevRankGraph = rankGraph
        val rPrb = {
          (srcs: Array[VertexId], id: VertexId) => resetProb * delta(srcs, id)
        }
        rankGraph = rankGraph.joinVertices(rankUpdates) {
          (id, oldRank, msgSum) => rPrb(srcIds, id) + (1.0 - resetProb) * msgSum
        }.cache()
        rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
        //println(s"PageRank finished iteration $iteration.")
        prevRankGraph.vertices.unpersist(false)
        prevRankGraph.edges.unpersist(false)
        iteration += 1
      }
      rankGraph
    }

  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphbasedFraudDetection")
    val sc = new SparkContext(conf)
    //val data = sc.textFile("Sheet11.csv")
    
    val datatemp = sc.parallelize(sc.textFile("hdfs://CGLAB-Linux-1:9000/user/mahdi/medicare.txt").take(10000))
    //val datatemp = sc.parallelize(sc.textFile("medicareProviders.txt").take(1000))
    val header = datatemp.first() 
    val data = datatemp.filter(x => x != header)

    //var vertices = data.map(line => (line.split(",")(0).toLong, (line.split(",")(2).concat(line.split(",")(3)))))
    var vertices = data.map{line =>
      var sp = line.split('\t')
      if(sp.length == 28)
        (sp(0).toLong, (sp(13)))
        else
          (-1.toLong,"")
      }
    vertices = vertices.filter(x => x._1 != -1).distinct()
      
    val temp2 = data.map { line =>
      val row = line.split('\t')
      //val row = line.split(",")
      if(row.length == 28)
      (row(0), row(16))
      //(row(0), row(4))
      else
        ("-1","-1")
    }
    val temp = temp2.filter(x => x._1 != "-1").distinct()
    temp.persist()
    val cartesian = temp.cartesian(temp)

    val filtered = cartesian.filter(x => (x._1._1 < x._2._1 && x._1._2 == x._2._2))

    val reduced = filtered.map(x => ((x._1._1, x._2._1), 1)).reduceByKey((a, b) => a + b)

    //reduced.foreach{x => println(x._1._1 + " , " + x._1._2 + " = " + x._2)}
    
    val high = reduced.filter(x => x._2 > 3)
    
    val edges = high.map(x => Edge(x._1._1.toLong, x._1._2.toLong, x._2))

    val graph: Graph[String, Int] = Graph(vertices, edges)
    graph.persist()
    val specialities = vertices.map(v => v._2)
    val specs = specialities.distinct()

    val drIdsForEachSpec = vertices.map(x => (x._2, x._1.toLong)).groupByKey()

    val temp4 = drIdsForEachSpec.toArray
    
    temp4.foreach { x =>
      val ranks = runWithOptions2(graph, 25, 0.2, x._2.toArray)

      val ranksBySpecs = vertices.join(ranks.vertices).map {
        case (id, (spec, rank)) => (id, (spec, rank))
      }

      val minRankOfSpec = ranksBySpecs.filter(s => s._2._1 == x._1).map(m => m._2._2).min()/2

      val r = ranksBySpecs.map { y =>
        if (x._1 != y._2._1) {
          if (y._2._2 >= minRankOfSpec) {
            val t ="SPEC X = " + x._1 + " SPEC Y " + y._2._1 + " ID id: " + y._1 + " Rank IS: " + y._2._2
            (1, t)
          } else
            (0, 0)
        } else
          (0, 0)
      }.groupByKey()
      if(r.count() != 0)
        r.filter(x => x._1 == 1).saveAsTextFile("hdfs://CGLAB-Linux-1:9000/user/mahdi/Result/" + x._1)
    }
  }
}
