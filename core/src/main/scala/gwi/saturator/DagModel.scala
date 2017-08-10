package gwi.saturator

import scala.math.Ordering

trait DagPartition {
  def pid: String
}

trait DagVertex {
  def vid: String
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

/**
  * !!! Test environment !!! To build Dag using just Ints instead of Vertices and Longs instead of Partitions
  */
protected[saturator] object DagMock {
  import scala.language.implicitConversions
  case class PartitionMock(pid: String) extends DagPartition
  case class VertexMock(vid: String) extends DagVertex

  implicit object VertexMockOrdering extends Ordering[DagVertex] {
    override def compare(x: DagVertex, y: DagVertex): Int =
      x.vid.compareTo(y.vid)
  }

  implicit object PartitionMockOrdering extends Ordering[DagPartition] {
    override def compare(x: DagPartition, y: DagPartition): Int =
      x.pid.compareTo(y.pid)
  }

  implicit def longToPartition(i: Long): DagPartition = PartitionMock(i.toString)
  implicit def intToPartition(i: Int): DagPartition = PartitionMock(i.toString)
  implicit def longsToPartitions(is: Set[Long]): Set[DagPartition] = is.map(implicitly[DagPartition](_))
  implicit def longsToPartitions(is: List[Long]): List[DagPartition] = is.map(implicitly[DagPartition](_))
  implicit def intToVertex(i: Int): DagVertex = VertexMock(i.toString)
  implicit def vertexEdgeFromInts(i: (Int,Int)): (DagVertex, DagVertex) = (i._1, i._2)
  implicit def longsToPartitionsByVertex(is: Map[Int, Set[Long]]): Map[DagVertex, Set[DagPartition]] = is.map { case (v,ps) => implicitly[DagVertex](v) -> implicitly[Set[DagPartition]](ps) }
  implicit def longsToPartitionsByVertex(is: List[(Int, List[Long])]): List[(DagVertex, List[DagPartition])] = is.map { case (v,ps) => implicitly[DagVertex](v) -> implicitly[List[DagPartition]](ps) }
  implicit def intsToStateByVertex(is: Map[Int, String]): Map[DagVertex, String] = is.map { case (v,s) => implicitly[DagVertex](v) -> s }
  implicit def intsToVertexStatesByPartition(is: Map[Int, Map[DagVertex, String]]): Map[DagPartition, Map[DagVertex, String]] = is.map { case (v,ps) => implicitly[DagPartition](v) -> ps }
}