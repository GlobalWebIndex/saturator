package gwi.s8

import scala.collection.immutable.TreeMap

package object impl {
  private[impl] type PartitionState = TreeMap[DagVertex, String]
  private[impl] type PartitionedDagState = TreeMap[DagPartition, PartitionState]
}
