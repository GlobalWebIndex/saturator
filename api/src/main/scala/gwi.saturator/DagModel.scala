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
    def serializedGraph: String
  }
  case class Progressing(serializedGraph: String) extends State
  case class Complete(serializedGraph: String) extends State
  case class Failed(serializedGraph: String) extends State
  object State {
    def apply(serializedGraph: String, states: Iterable[String]): State = states.toSet match {
      case xs if xs.size == 1 && xs.head == DagVertex.State.Complete => Complete(serializedGraph)
      case xs if xs.contains(DagVertex.State.Failed) => Failed(serializedGraph)
      case _ => Progressing(serializedGraph)
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