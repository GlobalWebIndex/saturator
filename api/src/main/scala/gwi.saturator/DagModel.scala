package gwi.saturator

import scala.collection.Iterable
import scala.math.Ordering

trait DagPartition {
  def pid: String
}

trait DagVertex {
  def vid: String
}

object DagPartition {
  sealed trait State {
    def serializedVertex: String
  }
  case class Progressing(serializedVertex: String) extends State
  case class Complete(serializedVertex: String) extends State
  case class Failed(serializedVertex: String) extends State
  object State {
    def apply(serializedVertex: String, states: Iterable[String]): State = states.toSet match {
      case xs if xs.size == 1 && xs.head == DagVertex.State.Complete => Complete(serializedVertex)
      case xs if xs.contains(DagVertex.State.Failed) => Failed(serializedVertex)
      case _ => Progressing(serializedVertex)
    }
  }
}

object DagVertex {
  protected[saturator] object State {
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