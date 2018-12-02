package gwi.s8.impl

import com.typesafe.scalalogging.StrictLogging
import gwi.s8.{DagPartition, DagVertex, Dependency}

import scala.collection.immutable.{SortedSet, TreeMap, TreeSet}
import scala.math.Ordering

private[impl] object PartitionedDagState extends StrictLogging {
  import PartitionState._

  private[impl] def apply(partitionsByVertex: Map[DagVertex, Set[DagPartition]])(implicit dag: Dag[DagVertex], po: Ordering[DagPartition], vo: Ordering[DagVertex]): PartitionedDagState = {
    val rootVertex = dag.root
    val rootVertexPartitions = partitionsByVertex(rootVertex)
    val vertexPartitions = partitionsByVertex.filterKeys(_ != rootVertex)

    def alienPartitionsError = {
      val (vertex, partitions) = vertexPartitions.find(_._2.diff(rootVertexPartitions).nonEmpty).map( t => t._1 -> t._2.diff(rootVertexPartitions)).get
      s"Graph contains $vertex with alien partitions:\n${partitions.mkString("\n","\n","\n")}" +
        s"Root partitions : ${rootVertexPartitions.mkString("\n","\n","\n")}Vertex partitions : ${vertexPartitions.find(_._2.diff(rootVertexPartitions).nonEmpty).map(_._2.mkString("\n","\n","\n"))}"
    }

    if(!vertexPartitions.forall(_._2.diff(rootVertexPartitions).isEmpty))
      logger.warn(alienPartitionsError)
    TreeMap(rootVertexPartitions.map(p => p -> PartitionState.buildGraphForPartition(p, rootVertex, partitionsByVertex)).toSeq:_*)
  }

  private[impl] implicit class PartitionedDagStatePimp(underlying: PartitionedDagState)(implicit dag: Dag[DagVertex], po: Ordering[DagPartition], vo: Ordering[DagVertex]) {

    private[impl] def handlePartitionOp(opName: String, p: DagPartition)(f: PartitionState => Either[String, PartitionState]): PartitionedDagState = underlying.get(p) match {
      case None =>
        logger.error(s"Operation '$opName' on partition $p failed because it doesn't exist !!!")
        underlying
      case Some(ps) =>
        f(ps) match {
          case Left(error) =>
            logger.error(s"Operation '$opName' on partition $p failed due to : $error \n${ps.printable}")
            underlying // TODO could we do something else than ignore it?
          case Right(newPs) =>
            logger.debug(s"Operation '$opName' on partition $p succeeded\n${ps.printable}\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newPs.printable}")
            underlying updated(p, newPs)
        }
    }

    private[impl] def addPartitions(partitions: TreeSet[DagPartition]): PartitionedDagState = {
      val (existingPartitions, nonExistingPartitions) = partitions.partition(underlying.contains)
      val partitionState = PartitionState()
      existingPartitions.foreach( p => logger.warn(s"Idempotently not inserting partition that already exist : $p") )
      nonExistingPartitions.foreach( p => logger.debug(s"Partition created : $p"))
      underlying ++ nonExistingPartitions.map(_ -> partitionState)
    }

    private[impl] def updatePartitions(partitions: TreeSet[DagPartition]): PartitionedDagState = {
      val (existingPartitions, nonExistingPartitions) = partitions.partition(underlying.contains) // updated partition should not be progressing
      nonExistingPartitions.foreach( p => logger.error(s"Partition cannot be changed because it is missing : $p") )
      existingPartitions.foreach( p => logger.debug(s"Partition changed : $p") )
      val newPartitionsDagState =
        existingPartitions.flatMap { p =>
          underlying(p).redo(dag.root) match {
            case Left(error) =>
              logger.error(s"Updating partition $p failed on $error")
              None
            case Right(ps) =>
              Some(p -> ps)
          }
        }
      underlying ++ newPartitionsDagState
    }

    private[impl] def redo(p: DagPartition, v: DagVertex): PartitionedDagState =
      handlePartitionOp("redo", p)(_.redo(v))

    private[impl] def fix(p: DagPartition): PartitionedDagState =
      handlePartitionOp("fix", p)(_.fix)

    private[impl] def fail(dep: Dependency): PartitionedDagState =
      handlePartitionOp("fail", dep.p)(_.fail(dep.sourceVertices, dep.targetVertex))

    private[impl] def succeed(dep: Dependency): PartitionedDagState =
      handlePartitionOp("succeed", dep.p)(_.succeed(dep.sourceVertices, dep.targetVertex))

    private[impl] def progress: PartitionedDagState =
      getPendingDeps.foldLeft(underlying) { case (acc, Dependency(p, sourceVertices, targetVertex)) =>
        acc.handlePartitionOp("progress", p)(_.progress(sourceVertices, targetVertex))
      }

    private[impl] def isSaturated: Boolean = underlying.values.forall(_.isSaturated)

    private[impl] def getPendingDeps: SortedSet[Dependency] =
      TreeSet(
        underlying.flatMap { case (p, partitionState) =>
          partitionState.getPendingVertices.map ( v => Dependency(p, dag.neighborAncestorsOf(v), v) )
        }.toSeq:_*
      )

    private[impl] def getProgressingDeps: SortedSet[Dependency] =
      TreeSet(
        underlying.flatMap { case (p, partitionState) =>
          partitionState.getProgressingVertices.map ( v => Dependency(p, dag.neighborAncestorsOf(v), v) )
        }.toSeq:_*
      )
  }

}