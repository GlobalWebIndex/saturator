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
  sealed trait S8InSysCmd extends S8IncomingMsg
  sealed trait S8InAppCmd extends S8IncomingMsg
  sealed trait S8InBulkCmd extends S8IncomingMsg

  case object GetState extends S8InSysCmd
  case object ShutDown extends S8InSysCmd

  case class RedoDagBranch(p: DagPartition, vertex: DagVertex) extends S8InBulkCmd
  case class FixPartition(p: DagPartition) extends S8InBulkCmd

  case class AckSaturation(dep: Dependency, succeeded: Boolean) extends S8InAppCmd
  sealed trait PartitionChanges extends S8InAppCmd {
    def partitions: TreeSet[DagPartition]
  }
  case class InsertPartitions(partitions: TreeSet[DagPartition]) extends PartitionChanges
  case class UpdatePartitions(partitions: TreeSet[DagPartition]) extends PartitionChanges

}

object out {
  sealed trait S8OutMsg extends S8Msg
  sealed trait S8OutCmd extends S8OutMsg
  sealed trait S8OutInfo extends S8OutMsg

  case object Saturated extends S8OutInfo
  case object Initialized extends S8OutInfo

  case class Saturate(dep: Dependency) extends S8OutCmd
  sealed trait GetPartitions extends S8OutCmd {
    def rootVertex: DagVertex
  }
  case class GetCreatedPartitions(rootVertex: DagVertex) extends GetPartitions
  case class GetChangedPartitions(rootVertex: DagVertex) extends GetPartitions
}
