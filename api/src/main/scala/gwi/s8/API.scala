package gwi.s8

import collection.immutable.{SortedSet, TreeSet}
import math.Ordering

case class DagPartition(pid: String)

case class DagVertex(vid: String)

object DagVertex {
  object State {
    val Pending = "Pending"
    val Complete = "Complete"
    val InProgress = "InProgress"
    val Failed = "Failed"
  }
}
case class Dependency(p: DagPartition, sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex)

object Dependency {
  implicit def dependencyOrdering(implicit po: Ordering[DagPartition], vo: Ordering[DagVertex]) = new Ordering[Dependency] {
    override def compare(x: Dependency, y: Dependency): Int = {
      val partitionOrd = po.compare(x.p,y.p)
      lazy val sourceVertexOrd = vo.compare(x.sourceVertices.head, y.sourceVertices.head)
      lazy val targetVertexOrd = vo.compare(x.targetVertex, y.targetVertex)
      if (partitionOrd != 0)
        partitionOrd
      else if (sourceVertexOrd != 0)
        sourceVertexOrd
      else
        targetVertexOrd
    }
  }

}

sealed trait S8Msg
sealed trait S8Cmd extends S8Msg

private[s8] sealed trait S8InternalCmd extends S8Cmd
private[s8] case class Initialize(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends S8InternalCmd

object in {
  sealed trait S8IncomingMsg extends S8Msg
  sealed trait S8IncomingCmd extends S8IncomingMsg

  case class AckSaturation(dep: Dependency, succeeded: Boolean) extends S8IncomingMsg

  case class RedoDagBranch(p: DagPartition, vertex: DagVertex) extends S8IncomingCmd
  case class FixPartition(p: DagPartition) extends S8IncomingCmd
  case class InsertPartitions(partitions: TreeSet[DagPartition]) extends S8IncomingCmd
  case class UpdatePartitions(partitions: TreeSet[DagPartition]) extends S8IncomingCmd
  case object GetState extends S8IncomingCmd
  case object ShutDown extends S8IncomingCmd
}

object out {
  sealed trait S8OutgoingMsg extends S8Msg
  sealed trait S8OutgoingCmd extends S8OutgoingMsg
  case object Saturated extends S8OutgoingMsg
  case object Initialized extends S8OutgoingMsg

  case class Saturate(dep: Dependency) extends S8OutgoingCmd
  case class GetPartitionChanges(rootVertex: DagVertex) extends S8OutgoingCmd
}
