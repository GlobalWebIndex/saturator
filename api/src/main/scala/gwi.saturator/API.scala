package gwi.saturator

import scala.math.Ordering

trait DagPartition {
  def pid: String
}

trait DagVertex {
  def vid: String
}

object DagVertex {
  object State {
    val Pending = "Pending"
    val Complete = "Complete"
    val InProgress = "InProgress"
    val Failed = "Failed"
  }
}
case class Dependency(p: DagPartition, sourceVertices: Set[DagVertex], targetVertex: DagVertex)

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

sealed trait SaturatorCmd
object SaturatorCmd {
  sealed trait Incoming extends SaturatorCmd
  sealed trait Outgoing extends SaturatorCmd
  private[saturator] sealed trait Internal extends SaturatorCmd

  case class Saturate(dep: Set[Dependency]) extends Outgoing
  case object Saturated extends Outgoing
  case object Initialized extends Outgoing
  private[saturator] case class Initialize(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends Internal

  case class CreatePartition(p: DagPartition) extends Incoming
  case class RedoDagBranch(p: DagPartition, vertex: DagVertex) extends Incoming
  case class FixPartition(p: DagPartition) extends Incoming
  case class SaturationResponse(dep: Dependency, succeeded: Boolean) extends Incoming
  case object GetState extends Incoming
  case object ShutDown extends Incoming
}
