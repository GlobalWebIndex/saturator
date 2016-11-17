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
  }
}
case class FailedDependency(p: DagPartition, sourceVertices: Set[DagVertex], targetVertex: DagVertex)
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