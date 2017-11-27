package gwi.s8

import scala.collection.immutable.TreeMap

package object impl {
  private[impl] type PartitionState = TreeMap[DagVertex, String]
  private[impl] type PartitionedDagState = TreeMap[DagPartition, PartitionState]

  private[impl] implicit class TreeMapPimp[K, V](underlying: TreeMap[K, V]) {
    def adjust(k: K)(f: V => V): TreeMap[K, V] = underlying.updated(k, f(underlying(k)))
    def flatAdjust(k: K)(f: Option[V] => Option[V]): TreeMap[K, V] = f(underlying.get(k)) match {
      case None =>
        underlying
      case Some(v) =>
        underlying updated(k, v)
    }
  }

}
