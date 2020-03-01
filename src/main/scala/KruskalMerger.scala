package SHAS

import org.apache.spark.rdd.RDD

import Auxiliary._

object KruskalMerger extends Serializable {
    // Union by rank
    def union(trees: Map[String, Tree], a: String, b: String): Unit = {
        val aroot: String = find(trees, a)
        val broot: String = find(trees, b)

        if(trees(aroot).rank < trees(broot).rank) {
            trees(aroot).parent = broot
        } else if(trees(aroot).rank > trees(broot).rank) {
            trees(broot).parent = aroot
        } else {
            trees(broot).parent = aroot
            trees(aroot).rank += 1
        }
    }

    // find root with path compression
    def find(trees: Map[String, Tree], v: String): String = {
        if(trees(v).parent != v) {
            // path compression: now v and its ancestors will point to the same parent
            trees(v).parent = find(trees, trees(v).parent)
        }

        trees(v).parent
    }

    def kruskalMerge(key: Int, msts: Iterable[MstEdge]): Array[(Int, MstEdge)] = {
        val E = msts.toArray.sortBy(_._3)
        val V = E.flatMap(x => Array(vdts(x._1), vdts(x._2))).distinct

        val rawtrees: Array[(String,Tree)] = new Array(V.length)
        var i = 0
        for(v <- V) {
            rawtrees(i) = (v, new Tree(v, 0))
            i += 1
        }
        i = 0
        val trees = rawtrees.toMap

        var mst: Array[(Int, MstEdge)] = new Array(V.length - 1)
        var edgesIncluded = 0

        while(edgesIncluded < V.length - 1 && i < E.length) {
            val nextMinEdge = E(i)
            i += 1
            val treeofX: String = find(trees, vdts(nextMinEdge._1))
            val treeofY: String = find(trees, vdts(nextMinEdge._2))

            // if X and Y do not belong to the same tree it is safe to add the edge in the MST
            // otherwise it creates a circuit
            if(treeofX != treeofY) {
                mst(edgesIncluded) = (key, nextMinEdge)
                edgesIncluded += 1
                union(trees, treeofX, treeofY)
            }
        }

        mst.filter(_ != null)
    }
}