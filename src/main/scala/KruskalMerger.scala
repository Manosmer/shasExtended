package SHAS

import org.apache.spark.rdd.RDD

import Auxiliary._

object KruskalMerger extends Serializable {
    // Union by rank
    def union(trees: collection.mutable.Map[String, Tree], a: String, b: String): Unit = {
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
    def find(trees: collection.mutable.Map[String, Tree], v: String): String = {
        if(trees(v).parent != v) {
            // path compression: now v and its ancestors will point to the same parent
            trees(v).parent = find(trees, trees(v).parent)
        }

        trees(v).parent
    }

    def kruskalMerge(key: Int, msts: Iterable[MstEdge]): Array[(Int, MstEdge)] = {
        val E = msts.toArray.sortBy(_._3)

        // initialize the trees of the forest
        var trees = collection.mutable.Map[String, Tree]()
        for(e <- E) {
            val v1 = vdts(e._1)
            val v2 = vdts(e._2)
            trees(v1) = new Tree(v1, 0)
            trees(v2) = new Tree(v2, 0)
        }
        val vSize = trees.keys.size
        var mst: Array[(Int, MstEdge)] = new Array(vSize - 1)
        
        var i = 0
        var edgesIncluded = 0
        while(edgesIncluded < vSize - 1 && i < E.length) {
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