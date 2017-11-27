package gwi.s8

import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable.{SortedSet, TreeMap, TreeSet}
import scala.math.Ordering

object PartitionedDagState extends StrictLogging {
  import PartitionState._

  def apply(partitionsByVertex: Map[DagVertex, Set[DagPartition]])(implicit dag: Dag[DagVertex], po: Ordering[DagPartition], vo: Ordering[DagVertex]): PartitionedDagState = {
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

  implicit class PartitionedDagStatePimp(underlying: PartitionedDagState)(implicit dag: Dag[DagVertex], po: Ordering[DagPartition], vo: Ordering[DagVertex]) {

    def addPartitions(partitions: TreeSet[DagPartition]): PartitionedDagState = {
      val (existingPartitions, nonExistingPartitions) = partitions.partition(underlying.contains)
      val partitionState = PartitionState()
      existingPartitions.foreach( p => logger.warn(s"Idempotently not inserting partition that already exist : $p") )
      nonExistingPartitions.foreach( p => logger.info(s"Partition created : $p"))
      underlying ++ nonExistingPartitions.map(_ -> partitionState)
    }

    def updatePartitions(partitions: TreeSet[DagPartition]): PartitionedDagState = {
      val (existingPartitions, nonExistingPartitions) = partitions.partition(underlying.contains) // updated partition should not be progressing
      nonExistingPartitions.foreach( p => logger.error(s"Partition cannot be changed because it is missing : $p") )
      existingPartitions.foreach( p => logger.error(s"Partition changed : $p") )
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

    def redo(p: DagPartition, v: DagVertex): PartitionedDagState =
      underlying.flatAdjust(p) {
        case None =>
          logger.error(s"Redoing $v of partition $p that doesn't exist !!!")
          None
        case Some(partitionState) =>
          partitionState.redo(v) match {
            case Left(error) =>
              logger.error(s"Redoing partition $p failed because : $error")
              None
            case Right(ps) =>
              logger.info(s"Partition redoing succeeded $p")
              Some(ps)
          }
      }

    def fix(p: DagPartition): PartitionedDagState =
      underlying.adjust(p) { partitionState =>
        partitionState.fix match {
          case Left(error) =>
            logger.info(s"Partition $p cannot be fixed due to $error")
            partitionState
          case Right(ps) =>
            logger.info(s"Partition fixing succeeded $p")
            ps
        }
      }

    def fail(dep: Dependency): PartitionedDagState = dep match {
      case Dependency(p, sourceVertices, targetVertex) =>
        underlying.adjust(p) { partitionState =>
          partitionState.fail(sourceVertices, targetVertex) match {
            case Left(error) =>
              logger.info(s"Partition $p cannot be failed due to $error")
              partitionState
            case Right(ps) =>
              logger.info(s"Partition failing succeeded $p")
              ps
          }
        }
    }

    def succeed(dep: Dependency): PartitionedDagState = dep match {
      case Dependency(p, sourceVertices, targetVertex) =>
        underlying.adjust(p) { partitionState =>
          partitionState.succeed(sourceVertices, targetVertex) match {
            case Left(error) =>
              logger.info(s"Partition $p cannot be succeeded due to $error")
              partitionState
            case Right(ps) =>
              logger.info(s"Partition succeeding succeeded $p")
              ps
          }
        }
    }

    def progress: PartitionedDagState =
      getPendingDeps.foldLeft(underlying) { case (acc, Dependency(p, sourceVertices, targetVertex)) =>
        acc.adjust(p) { partitionState =>
          partitionState.progress(sourceVertices, targetVertex) match {
            case Left(error) =>
              logger.info(s"Partition $p cannot progress due to $error")
              partitionState
            case Right(ps) =>
              logger.info(s"Partition progressing succeeded $p")
              ps
          }
        }
      }

    def isSaturated: Boolean = underlying.values.forall(_.isSaturated)

    def getPendingDeps: SortedSet[Dependency] =
      TreeSet(
        underlying.flatMap { case (p, partitionState) =>
          partitionState.getPendingVertices.map ( v => Dependency(p, dag.neighborAncestorsOf(v), v) )
        }.toSeq:_*
      )

    def getProgressingDeps: SortedSet[Dependency] =
      TreeSet(
        underlying.flatMap { case (p, partitionState) =>
          partitionState.getProgressingVertices.map ( v => Dependency(p, dag.neighborAncestorsOf(v), v) )
        }.toSeq:_*
      )
  }

}