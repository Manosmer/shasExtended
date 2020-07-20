package SHAS

import math.{pow, sqrt}


object Auxiliary extends Serializable {
    val INF = Double.MaxValue

    type MstEdge = (Array[Double], Array[Double], Double)

    /** Calculates the euclidean distance of any pair of two vertices.
     * 
     * @return the calculated distance.
     */
    def dist(v1: Array[Double], v2: Array[Double]): Double = {
        var result = 0.0
        for(i <- 0 until v1.length) {
            result += Math.pow(v1(i) - v2(i), 2)
        }
        math.sqrt(result)
    }

    /** Creates a string that represents a vertex.
     * 
     * @return a vertex as a string
     */
    def vdts(v: Array[Double]): String = {
        v.mkString("|")
    }

    /** Deserializes a string that represents a vertex into its endpoints, splitting it 
     * by the character '|'.
     * 
     * @return a vertex as an Array[Double]
     */
    def vstd(v: String): Array[Double] = {
        v.split('|').map(_.toDouble)
    }

    /** Computes the key of the bipartite subgraph's mst.
     * 
     * The key is computed to satisfy the need for consecutive mst keys. While the complete subgraphs'
     * msts fill the first numSplits positions, the complete bipartite subgraphs' msts need to fill 
     * the consecutive positions from numSplits till the end in order to be correctly partitioned 
     * in the merge phase.
     * 
     * The key of the bipartite subgraph's mst is produced by finding the numerical representation of 
     * "k1"+"k2" and shifting it by numsplits - Σ(k1+1). That way, all bipartite submsts will have a unique 
     * key between numsplits and total num of submsts.
     * 
     * @return the key of the bipartite subgraph's mst.
     */
    def bipartitekey(k1: Int, k2: Int, numSplits: Int): Int = {
        var shift = 0
        // - Σ_k1+1(i)
        for(k <- 1 to (k1+1)) {
            shift -= k
        }

        // Σ_k1+1(numsplits - i)
        shift += (k1+1) * numSplits
        shift + k2
    }

    /** Tree class, representing the trees of a forest for kruskal
     */
    class Tree {
        var parent: String = ""
        var rank: Int = -1
    
        def this(p: String, r: Int) = {
            this()
            this.parent = p
            this.rank = r
        }
    }

    /** Header class for the vertices of Prim's algo, containg useful info.
     * 
     * Fields
     * distance: the value utilized for determining which vertex to choose next
     * parent: the endpoint to which the current vertex was recorded to be connected
     */
    class primVertexHeader {
        var distance: Double = INF
        var parent: String = ""

        def this(d: Double, p: String) = {
            this()
            this.distance = d
            this.parent = p
        }

        def changeDis(d: Double, p: String) = {
            this.distance = d
            this.parent = p
        }
    }

    /** Returns the unused vertex with the minimum distance
     * 
     * @return the string form of the vertex
     */
    def getMinV(mapper: collection.mutable.Map[String, primVertexHeader]): String = {
        var minV = ""
        var minDist = INF
        
        // find nearest vertex
        for((k,v) <- mapper) {
            if(v.distance < minDist) {
                minV = k
                minDist = v.distance
            }
        }
       minV
    }
}
