package gwi.s8

private[s8] trait DagTestSupport {
  import language.implicitConversions

  private[s8] implicit object intVertexOrdering extends Ordering[DagVertex] {
    override def compare(x: DagVertex, y: DagVertex): Int =
      x.vid.toInt.compareTo(y.vid.toInt)
  }

  private[s8] implicit object intPartitionOrdering extends Ordering[DagPartition] {
    override def compare(x: DagPartition, y: DagPartition): Int =
      x.pid.toInt.compareTo(y.pid.toInt)
  }

  private[s8] implicit def longToPartition(i: Long): DagPartition = DagPartition(i.toString)
  private[s8] implicit def intToPartition(i: Int): DagPartition = DagPartition(i.toString)
  private[s8] implicit def longsToPartitions(is: Set[Long]): Set[DagPartition] = is.map(implicitly[DagPartition](_))
  private[s8] implicit def longsToPartitions(is: List[Long]): List[DagPartition] = is.map(implicitly[DagPartition](_))
  private[s8] implicit def intToVertex(i: Int): DagVertex = DagVertex(i.toString)
  private[s8] implicit def vertexEdgeFromInts(i: (Int,Int)): (DagVertex, DagVertex) = (i._1, i._2)
  private[s8] implicit def longsToPartitionsByVertex(is: Map[Int, Set[Long]]): Map[DagVertex, Set[DagPartition]] = is.map { case (v,ps) => implicitly[DagVertex](v) -> implicitly[Set[DagPartition]](ps) }
  private[s8] implicit def longsToPartitionsByVertex(is: List[(Int, List[Long])]): List[(DagVertex, List[DagPartition])] = is.map { case (v,ps) => implicitly[DagVertex](v) -> implicitly[List[DagPartition]](ps) }
  private[s8] implicit def intsToStateByVertex(is: Map[Int, String]): Map[DagVertex, String] = is.map { case (v,s) => implicitly[DagVertex](v) -> s }
  private[s8] implicit def intsToVertexStatesByPartition(is: Map[Int, Map[DagVertex, String]]): Map[DagPartition, Map[DagVertex, String]] = is.map { case (v,ps) => implicitly[DagPartition](v) -> ps }
}