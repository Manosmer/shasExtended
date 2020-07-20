package SHAS

import org.apache.spark.sql.SparkSession
import math.{ceil, pow, log}

import PrimLocalMST._
import PrimLMDense._
import KruskalMerger._
import Auxiliary._



object Main extends Serializable {
    val r = new scala.util.Random
    /** Associates a given vertex to a split uniformly and randomly.
      *
      * @param v the given vertex
      * @param k the number of splits
      * @return
      */
    def partition(v: Array[Double], k: Int): (Int, Array[Double]) = {
        val partitionKey = r.nextInt(k)

        (partitionKey, v)
    }

    def vertexMap(v: String, k: Int): (String, Int) = {
        val partitionKey = r.nextInt(k)

        (v, partitionKey)
    }

    def edgeMap(e: MstEdge, mapper: Map[String, Int]): ((Int, Int), MstEdge) = {
        val k1 = mapper(vdts(e._1))
        val k2 = mapper(vdts(e._2))

        if(k1 <= k2) {
            return ((k1, k2), e)
        } else {
            return ((k2, k1), e)
        }
    }

    def main(args: Array[String]): Unit = {
        // read cmd arguments
        val mode = args(0)
        val inputPath = args(1)
        val numInPartition = args(2).toDouble
        val mergeFactor = args(3).toInt

        val cpus = args(4).toInt

        val spark = SparkSession.builder
            .master("local["+ cpus +"]")
            .appName("Extended SHAS cpus:" + cpus + "|D:" + inputPath +"|partNum:" + numInPartition + "|mergeF:" + mergeFactor)
            .getOrCreate

        val sc = spark.sparkContext
        import spark.implicits._ 

        var iterations = 0
        if(mode == "-c") {

            val vertices = sc.textFile(inputPath).map(vstd)
            val n = vertices.count
            val k = (math.ceil(n.toDouble / numInPartition)).toInt
    
            // split vertices into subgraphs
            val subgraphs = vertices.map(x => partition(x, k)).groupByKey()
    
            // compute MSTs of subgraphs
            val completeSubgraphsMSTs = subgraphs.flatMap(x => primMST(x._2, x._1))
            val bipartiteCompleteSubgraphsMSTs = subgraphs.cartesian(subgraphs).filter(x => x._1._1 < x._2._1).flatMap(x => primBipartiteMST(x._1._1, x._2._1, x._1._2, x._2._2, k))
            
            var allMST = completeSubgraphsMSTs.union(bipartiteCompleteSubgraphsMSTs)
            
            // merge phase
            var allMSTnum = k + (k * (k - 1) / 2) 
            while(allMSTnum > 1) {
                iterations += 1
                // num of buckets of mergeFactor msts
                val l = math.ceil(allMSTnum.toDouble / mergeFactor).toInt
    
                // fill the l buckets / distribute based on hash
                val hashMSTs = allMST.map(x => (x._1 % l, x._2))
                // merge
                allMST = hashMSTs.groupByKey().flatMap(x => kruskalMerge(x._1, x._2))
                allMSTnum = l
            }
    
            // print final mst
            val finalMST = allMST.collect()
            println(finalMST.map(_._2._3).reduce(_ + _))
        }
        else if(mode == "-d") {
            val edges = sc.textFile(inputPath).map(x => {
                var t = x.split(',')
                (vstd(t(0)), vstd(t(1)), t(2).toDouble)
            })
            val vertices = edges.flatMap(e => Array(vdts(e._1), vdts(e._2))).distinct
            val n = vertices.count
            val k = (math.ceil(n.toDouble / numInPartition)).toInt

            // create mapper to partition each edge based on endpoints
            val vertexMapper = vertices.map(vertexMap(_, k)).collect.toMap

            val broadcastMapper = sc.broadcast(vertexMapper)

            // find local MSTs
            var allMST = edges.map(edgeMap(_, broadcastMapper.value)).groupByKey().flatMap(x => denseMST(x._1, x._2, k))

            // merge phase
            var allMSTnum = k + (k * (k - 1) / 2) 
            while(allMSTnum > 1) {
                iterations += 1
                // num of buckets of mergeFactor msts
                val l = math.ceil(allMSTnum.toDouble / mergeFactor).toInt
    
                // fill the l buckets / distribute based on hash
                val hashMSTs = allMST.map(x => (x._1 % l, x._2))
                // merge
                allMST = hashMSTs.groupByKey().flatMap(x => kruskalMerge(x._1, x._2))
                allMSTnum = l
            }
    
            // print final mst
            val finalMST = allMST.collect()
            println(finalMST.map(_._2._3).reduce(_ + _))
        }
        println(iterations)
        sc.stop()
    }

}