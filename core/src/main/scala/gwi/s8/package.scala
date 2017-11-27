package gwi

import scala.collection.immutable.TreeMap

package object s8 {
  type PartitionState = TreeMap[DagVertex, String]
  type PartitionedDagState = TreeMap[DagPartition, PartitionState]

  implicit class TreeMapPimp[K, V](underlying: TreeMap[K, V]) {
    def adjust(k: K)(f: V => V): TreeMap[K, V] = underlying.updated(k, f(underlying(k)))
    def flatAdjust(k: K)(f: Option[V] => Option[V]): TreeMap[K, V] = f(underlying.get(k)) match {
      case None =>
        underlying
      case Some(v) =>
        underlying updated(k, v)
    }
  }

}
