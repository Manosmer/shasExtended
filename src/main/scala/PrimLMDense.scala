package SHAS

import Auxiliary._

object PrimLMDense extends Serializable {
    /**
      * Driver function that calls primForEdges for local mst computation and creates the mst's key. 
      *
      * @param compositeKey the key of the edges
      * @param E the given edges
      * @param numSplits the number of splits of the vertices (used to create the key of the mst)
      * 
      * @return the mst of the given vertices with its corresponding key
      */
    def denseMST(compositeKey: (Int, Int), E: Iterable[MstEdge], numSplits: Int): Array[(Int, MstEdge)] = {
        if(compositeKey._1 == compositeKey._2) {
            primForEdges(compositeKey._1, E)
        } else {
            primForEdges(bipartitekey(compositeKey._1, compositeKey._2, numSplits), E)
        }
    }

    /**
      * Computes the local mst based on the given edges using Prim's algo. 
      *
      * @param key the key of the mst
      * @param E the given edges
      * 
      * @return the edges of the mst mapped by the key.
      */
    private def primForEdges(key: Int, E: Iterable[MstEdge]): Array[(Int, MstEdge)] = {
        var it = E.iterator
        var edgeMapper = collection.mutable.Map[String, Array[MstEdge]]()

        // create adjacency list
        while(it.hasNext) {
            val e = it.next()
            if(!edgeMapper.contains(vdts(e._1))) {
                edgeMapper(vdts(e._1)) = Array()
            }
            edgeMapper(vdts(e._1)) = edgeMapper(vdts(e._1)) :+ e
            if(!edgeMapper.contains(vdts(e._2))) {
                edgeMapper(vdts(e._2)) = Array()
            }
            edgeMapper(vdts(e._2)) = edgeMapper(vdts(e._2)) :+ e
        }
        
        var vertexDistMapper = collection.mutable.Map(edgeMapper.keys.toArray.map(x => (x, new primVertexHeader(INF, ""))):_*)
        
        val vertexNum = vertexDistMapper.keys.size
        var mst: Array[(Int, MstEdge)] = new Array(vertexNum - 1)
        // first vertex as a starting point
        var keyIt = vertexDistMapper.keysIterator
        var nextV = keyIt.next()
        vertexDistMapper -= nextV

        // find MST
        for(mstcnt <- 0 until vertexNum - 1) {

            for(adjedge <- edgeMapper(nextV)) {
                // find the destination point, nextV is the source endpoint 
                var destpoint = ""
                if(vdts(adjedge._1) == nextV) {
                    destpoint = vdts(adjedge._2)
                } else {
                    destpoint = vdts(adjedge._1)
                }
                // if destination is has not been selected yet
                if(vertexDistMapper.contains(destpoint)) {
                    var bheader = vertexDistMapper(destpoint)
                    val pairdist = dist(vstd(nextV), vstd(destpoint))
                    if(pairdist < bheader.distance) {
                        bheader.changeDis(pairdist, nextV)
                    }
                }
            }

            // implementation for disconnected graph
            // find next min vertex and the corresponding edge
            val minV = getMinV(vertexDistMapper)
            if(minV != "") {
                // if min exists (connected component not yet traversed fully)
                
                val minheader = vertexDistMapper(minV)
                nextV = minV
                // each edge in mst is in the a form of (parent, minEdge, distance)
                mst(mstcnt) = (key, (vstd(minheader.parent), vstd(nextV), minheader.distance))
                vertexDistMapper -= minV
            } else {
                // hop to a new connected component
                
                nextV = vertexDistMapper.keysIterator.next()
            }
        }

        // Erase null positions (disconnected graph produces less edges than v - 1)
        mst.filter(_ != null)
    }
}