package gwi.saturator

/**
  * !!! Test environment !!! it is needed in core tests and example projects to, "test->compile" dependency is awful
  * To build Dag using just Ints instead of Vertices and Longs instead of Partitions
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