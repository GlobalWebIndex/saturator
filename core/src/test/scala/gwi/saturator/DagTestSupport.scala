package gwi.saturator

trait DagTestSupport {
  import scala.language.implicitConversions

  implicit object intVertexOrdering extends Ordering[DagVertex] {
    override def compare(x: DagVertex, y: DagVertex): Int =
      x.vid.toInt.compareTo(y.vid.toInt)
  }

  implicit object intPartitionOrdering extends Ordering[DagPartition] {
    override def compare(x: DagPartition, y: DagPartition): Int =
      x.pid.toInt.compareTo(y.pid.toInt)
  }

  implicit def longToPartition(i: Long): DagPartition = DagPartition(i.toString)
  implicit def intToPartition(i: Int): DagPartition = DagPartition(i.toString)
  implicit def longsToPartitions(is: Set[Long]): Set[DagPartition] = is.map(implicitly[DagPartition](_))
  implicit def longsToPartitions(is: List[Long]): List[DagPartition] = is.map(implicitly[DagPartition](_))
  implicit def intToVertex(i: Int): DagVertex = DagVertex(i.toString)
  implicit def vertexEdgeFromInts(i: (Int,Int)): (DagVertex, DagVertex) = (i._1, i._2)
  implicit def longsToPartitionsByVertex(is: Map[Int, Set[Long]]): Map[DagVertex, Set[DagPartition]] = is.map { case (v,ps) => implicitly[DagVertex](v) -> implicitly[Set[DagPartition]](ps) }
  implicit def longsToPartitionsByVertex(is: List[(Int, List[Long])]): List[(DagVertex, List[DagPartition])] = is.map { case (v,ps) => implicitly[DagVertex](v) -> implicitly[List[DagPartition]](ps) }
  implicit def intsToStateByVertex(is: Map[Int, String]): Map[DagVertex, String] = is.map { case (v,s) => implicitly[DagVertex](v) -> s }
  implicit def intsToVertexStatesByPartition(is: Map[Int, Map[DagVertex, String]]): Map[DagPartition, Map[DagVertex, String]] = is.map { case (v,ps) => implicitly[DagPartition](v) -> ps }
}