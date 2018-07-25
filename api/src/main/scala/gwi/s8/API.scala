package gwi.s8

import collection.immutable.{SortedSet, TreeMap, TreeSet}
import math.Ordering
import scala.concurrent.duration._

/** Partition and Vertex should ideally be just a String with type tag https://github.com/rudogma/scala-supertagged
  * This way saturator users must convert between their and this type
  */
case class DagPartition(pid: String)

case class DagVertex(vid: String)

/** Represent a dependency to be saturated, vertex can depend on multiple source vertices but not the other way around */
case class Dependency(p: DagPartition, sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex)

object Dependency {
  /** Default dependency ordering is exposed at global level and could be possibly overriden by custom ordering according to user needs */
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

/** Saturator requests user for created and changed partitions in the system so that it can saturate DAG for that particular partition */
sealed trait PartitionCheck {
  def interval: FiniteDuration
  def delay: FiniteDuration
}
object PartitionCheck {
  def created(interval: FiniteDuration, delay: FiniteDuration) = CreatedPartitionCheck(interval, delay)
  def changed(interval: FiniteDuration, delay: FiniteDuration) = ChangedPartitionCheck(interval, delay)
}

/** Check for create/new partitions produced by user system, saturator then creates a new DAG layer for each new partition and saturates all dependencies */
case class CreatedPartitionCheck(interval: FiniteDuration, delay: FiniteDuration) extends PartitionCheck

/** Check for partitions that have changed in the user system since the last check, saturator then saturates all dependencies in existing partition DAG layer for those partitions */
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

sealed trait MsgContainer[T <: S8Msg] {
  def msg: T
  def vertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]]
  def depsInFlight: Set[Dependency]
}

/** system messages don't leave the boundaries of saturator */
private[s8] object system {
  private[s8] sealed trait S8InternalCmd extends S8Msg
  private[s8] case class Initialize(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends S8InternalCmd
  private[s8] case class Submit(cmd: out.S8OutCmd) extends S8InternalCmd
}

/** Messages that are coming from user system to saturator */
object in {
  sealed trait S8IncomingMsg extends S8Msg
  sealed trait S8InSysCmd extends S8IncomingMsg
  sealed trait S8InAppCmd extends S8IncomingMsg
  sealed trait S8InBulkCmd extends S8IncomingMsg

  case object GetState extends S8InSysCmd
  case object ShutDown extends S8InSysCmd

  case class RedoDagBranch(p: DagPartition, vertex: DagVertex) extends S8InBulkCmd
  case class FixPartition(p: DagPartition) extends S8InBulkCmd
  case class RecreatePartition(p: DagPartition, rootVertex: DagVertex) extends S8InBulkCmd

  case class AckSaturation(dep: Dependency, succeeded: Boolean) extends S8InAppCmd
  sealed trait PartitionChanges extends S8InAppCmd {
    def partitions: TreeSet[DagPartition]
  }
  case class InsertPartitions(partitions: TreeSet[DagPartition]) extends PartitionChanges
  case class UpdatePartitions(partitions: TreeSet[DagPartition]) extends PartitionChanges

}

/** Messages that are outgoing from saturator to the user system */
object out {
  sealed trait S8OutMsg extends S8Msg
  sealed trait S8OutCmd extends S8OutMsg
  sealed trait S8OutInfo extends S8OutMsg

  case class Issued(msg: S8OutMsg, vertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]], depsInFlight: Set[Dependency]) extends MsgContainer[S8OutMsg]
  case class Submitted(msg: in.S8IncomingMsg, vertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]], depsInFlight: Set[Dependency]) extends MsgContainer[in.S8IncomingMsg]

  case object Saturated extends S8OutInfo
  case object Initialized extends S8OutInfo

  case class Saturate(dep: Dependency) extends S8OutCmd
  sealed trait GetPartitions extends S8OutCmd {
    def rootVertex: DagVertex
  }
  case class GetCreatedPartitions(rootVertex: DagVertex) extends GetPartitions
  case class GetChangedPartitions(rootVertex: DagVertex) extends GetPartitions
  case class GetRecreatedPartition(p: DagPartition, rootVertex: DagVertex) extends GetPartitions
}
