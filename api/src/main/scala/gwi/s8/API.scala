package gwi.s8

import collection.immutable.{SortedSet, TreeSet}
import math.Ordering
import scala.concurrent.duration._

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

sealed trait PartitionCheck {
  def interval: FiniteDuration
  def delay: FiniteDuration
}
object PartitionCheck {
  def created(interval: FiniteDuration, delay: FiniteDuration) = CreatedPartitionCheck(interval, delay)
  def changed(interval: FiniteDuration, delay: FiniteDuration) = ChangedPartitionCheck(interval, delay)
}
case class CreatedPartitionCheck(interval: FiniteDuration, delay: FiniteDuration) extends PartitionCheck
case class ChangedPartitionCheck(interval: FiniteDuration, delay: FiniteDuration) extends PartitionCheck

case class Schedule(createdPartitionCheck: Option[CreatedPartitionCheck], changedPartitionCheck: Option[ChangedPartitionCheck])

object Schedule {
  def noop = Schedule(None, None)
}

trait S8LauncherSupport {
  import org.backuity.clist.util.Read

  implicit def createdPartitionCheckRead: Read[CreatedPartitionCheck] = Read.reads("created-partition-check") { str =>
    val Array(interval, delay) = str.split(":").filter(_.nonEmpty).map(_.toInt)
    PartitionCheck.created(interval.seconds, delay.seconds)
  }

  implicit def changedPartitionCheckRead: Read[ChangedPartitionCheck] = Read.reads("changed-partition-check") { str =>
    val Array(interval, delay) = str.split(":").filter(_.nonEmpty).map(_.toInt)
    PartitionCheck.changed(interval.seconds, delay.seconds)
  }
}

sealed trait S8Msg
sealed trait S8Cmd extends S8Msg

private[s8] sealed trait S8InternalCmd extends S8Cmd
private[s8] case class Initialize(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends S8InternalCmd

object in {
  sealed trait S8IncomingMsg extends S8Msg
  sealed trait S8IncomingCmd extends S8IncomingMsg
  sealed trait S8IncomingInfo extends S8IncomingMsg

  case class AckSaturation(dep: Dependency, succeeded: Boolean) extends S8IncomingInfo

  case class RedoDagBranch(p: DagPartition, vertex: DagVertex) extends S8IncomingCmd
  case class FixPartition(p: DagPartition) extends S8IncomingCmd
  case object GetState extends S8IncomingCmd
  case object ShutDown extends S8IncomingCmd

  sealed trait PartitionChanges extends S8IncomingCmd {
    def partitions: TreeSet[DagPartition]
  }
  case class InsertPartitions(partitions: TreeSet[DagPartition]) extends PartitionChanges
  case class UpdatePartitions(partitions: TreeSet[DagPartition]) extends PartitionChanges
}

object out {
  sealed trait S8OutgoingMsg extends S8Msg
  sealed trait S8OutgoingCmd extends S8OutgoingMsg
  sealed trait S8OutgoingInfo extends S8OutgoingMsg

  case object Saturated extends S8OutgoingInfo
  case object Initialized extends S8OutgoingInfo

  case class Saturate(dep: Dependency) extends S8OutgoingCmd
  sealed trait GetPartitions extends S8OutgoingCmd {
    def rootVertex: DagVertex
  }
  case class GetCreatedPartitions(rootVertex: DagVertex) extends GetPartitions
  case class GetChangedPartitions(rootVertex: DagVertex) extends GetPartitions
}
