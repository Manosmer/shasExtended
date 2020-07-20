package SHAS

import Auxiliary._

object PrimLocalMST extends Serializable{
    /** Finds the local mst of a given set of vertices, implementing Prim's algo.
     * 
     * @param v the set of vertices
     * @param key the split id
     * @return the set of edges which constitute the MST mapped by the key (split id).
     */
    def primMST(v: Iterable[Array[Double]], key: Int): Array[(Int, MstEdge)] = {
        // the MST
        var mst: Array[(Int, MstEdge)] = new Array(v.size - 1)

        var it = v.iterator
        // create a map of vertex -> primVertexHeader for every vertex
        var vertexDistMapper = collection.mutable.Map.empty[String, primVertexHeader]
        while(it.hasNext) {
            val nextV = it.next()
            vertexDistMapper += (vdts(nextV) -> new primVertexHeader(INF, ""))
        }

        // first vertex of graph as a starting point
        var nextV = v.iterator.next()
        vertexDistMapper -= vdts(nextV)

        for(mstcnt <- 0 until v.size - 1) {
            // populate all the edges from nextV
            var it = vertexDistMapper.keysIterator

            // populate and update edges from nextV to each vertex b
            while(it.hasNext) {
                var bkey = it.next()
                var bheader = vertexDistMapper(bkey)
                val pairdist = dist(nextV, vstd(bkey))
                if(pairdist < bheader.distance) {
                    bheader.changeDis(pairdist, vdts(nextV))
                }
            }

            // find next min edge and the corresponding vertex
            val minV = getMinV(vertexDistMapper)
            val minheader = vertexDistMapper(minV)
            nextV = vstd(minV)
            // each edge in mst is in the a form of (parent, minEdge, distance)
            mst(mstcnt) = (key, (vstd(minheader.parent), nextV, minheader.distance))
            vertexDistMapper -= minV
        }

        mst
    }

    /** Used like the primMST function but tweaked for complete bipartite graphs.
     * 
     * @param k1 the first key of the bipartite graph
     * @param k2 the second key of the bipartite graph
     * @param v1 the vertices of the left complete subgraph
     * @param v2 the vertices of the right complete subgraph

     * @return the set of edges which constitute the MST mapped by the key (created by Auxiliary.bipartiteKey).
     */ 
    def primBipartiteMST(k1: Int, k2: Int, v1: Iterable[Array[Double]], v2: Iterable[Array[Double]], numSplits: Int): Array[(Int, MstEdge)] = {
        val key = bipartitekey(k1, k2, numSplits)
        var mst: Array[(Int, MstEdge)] = new Array(v1.size + v2.size - 1)

        // create mappers for each subset of the bipartite graph
        var dMapper1 = collection.mutable.Map.empty[String, primVertexHeader]
        var dMapper2 = collection.mutable.Map.empty[String, primVertexHeader]

        // mapper for left graph
        var it1 = v1.iterator
        while(it1.hasNext) {
            val nextV = it1.next()
            dMapper1 += (vdts(nextV) -> new primVertexHeader(INF, ""))
        }
        
        // mapper for right graph
        var it2 = v2.iterator
        while(it2.hasNext) {
            val nextV = it2.next()
            dMapper2 += (vdts(nextV) -> new primVertexHeader(INF, ""))
        }

        // first left vertex as a starting point
        var nextV = v1.iterator.next()
        // remove vertex from the set of available for inclusion
        dMapper1 -= vdts(nextV)

        // populate edges towards the right subset from the first vertex
        var it = dMapper2.keysIterator
        while(it.hasNext) {
            var bkey = it.next()
            var bheader = dMapper2(bkey)
            val pairdist = dist(nextV, vstd(bkey))
            if(pairdist < bheader.distance) {
                bheader.changeDis(pairdist, vdts(nextV))
            }
        }

        
        val minV = getMinV(dMapper2)
        nextV = vstd(minV)
        var minheader = dMapper2(minV)
        mst(0) = (key, (vstd(minheader.parent), nextV, minheader.distance))
        dMapper2 -= minV

        var left2right: Boolean = false
        for(mstcnt <- 1 until (v1.size + v2.size - 1)) {
            if(left2right) {
                // populate and update edges from left(nextV) to right(dUMapper2)
                var it = dMapper2.keysIterator
                
                while(it.hasNext) {
                    var bkey = it.next()
                    var bheader = dMapper2(bkey)
                    val pairdist = dist(nextV, vstd(bkey))
                    if(pairdist < bheader.distance) {
                        bheader.changeDis(pairdist, vdts(nextV))
                    }
                }
                
            } else {
                // populate and update edges from right(nextV) to left(dUMapper1)
                var it = dMapper1.keysIterator
                
                while(it.hasNext) {
                    var bkey = it.next()
                    var bheader = dMapper1(bkey)
                    val pairdist = dist(nextV, vstd(bkey))
                    if(pairdist < bheader.distance) {
                        bheader.changeDis(pairdist, vdts(nextV))
                    }
                }
            }
            
            // find the shortest edge from both subsets
            val minV1 = getMinV(dMapper1)
            // if left graph not empty get header. Placeholder otherwise.
            var minv1header = new primVertexHeader(INF, "")
            if(minV1 != "") {
                minv1header = dMapper1(minV1)
            }
            val minV2 = getMinV(dMapper2)
            // if right graph not empty get header. Placeholder otherwise.
            var minv2header = new primVertexHeader(INF, "")
            if(minV2 != "") {
                minv2header = dMapper2(minV2)
            }
            if(minv1header.distance <= minv2header.distance) {
                // next is picked from the left subset
                nextV = vstd(minV1)
                mst(mstcnt) = (key, (vstd(minv1header.parent), nextV, minv1header.distance))
                dMapper1 -= minV1
                left2right = true // next round populate from left
            } else {
                // next is picked from the right subset
                nextV = vstd(minV2)
                mst(mstcnt) = (key, (vstd(minv2header.parent), nextV, minv2header.distance))
                dMapper2 -= minV2
                left2right = false // next round populate from right
            }
        }
        mst
    }
}