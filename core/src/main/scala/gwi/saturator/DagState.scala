package gwi.saturator

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.math.Ordering

object DagState {
  implicit class TreeMapPimp[A, B](underlying: TreeMap[A, B]) {
    def adjust(k: A)(f: B => B) = underlying.updated(k, f(underlying(k)))
  }

  def empty(implicit po: Ordering[DagPartition], vo: Ordering[DagVertex]): DagState = DagState(vertexStatesByPartition = TreeMap.empty[DagPartition, TreeMap[DagVertex, String]], failedDependencies = Set.empty)

  trait DagStateEvent
  case class StateInitializedEvent(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends DagStateEvent
  case class SaturationInitializedEvent(deps: Set[Dependency]) extends DagStateEvent
  case class SaturationSucceededEvent(dep: Dependency) extends DagStateEvent
  case class SaturationFailedEvent(dep: Dependency) extends DagStateEvent
  case class PartitionCreatedEvent(p: DagPartition) extends DagStateEvent
  case class PartitionVertexRemovedEvent(p: DagPartition, vertex: DagVertex) extends DagStateEvent
}

case class DagState private(
                             private val vertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]],
                             private val failedDependencies: Set[FailedDependency]
              ) {
  import Dag._
  import DagState._
  import DagVertex.State._

  private def alienPartitionsError(vertexPartitions: Map[DagVertex, Set[DagPartition]], rootVertexPartitions: Set[DagPartition]) = {
    s"Graph contains alien partitions:\n${pprint.tokenize(vertexPartitions.find(_._2.diff(rootVertexPartitions).nonEmpty).map( t => t._1 -> t._2.diff(rootVertexPartitions)).get).mkString("\n","\n","\n")}" +
    s"Root partitions : ${rootVertexPartitions.mkString("\n","\n","\n")}Vertex partitions : ${vertexPartitions.find(_._2.diff(rootVertexPartitions).nonEmpty).map(_._2.mkString("\n","\n","\n"))}"
  }

  def getVertexStatesByPartition: Map[DagPartition, Map[DagVertex, String]] = vertexStatesByPartition
  def getFailedDependencies: Set[FailedDependency] = failedDependencies
  def getVertexStatesFor(p: DagPartition) = vertexStatesByPartition(p)
  def isSaturated = vertexStatesByPartition.values.forall(_.values.forall(_ == Complete))
  override def toString = pprint.tokenize(vertexStatesByPartition).mkString

  def getPendingToProgressPartitions(edges: Set[(DagVertex, DagVertex)])(implicit po: Ordering[DagPartition], vo: Ordering[DagVertex]): Set[Dependency] =
    TreeSet(
      vertexStatesByPartition.flatMap { case (p, partitionStateByVertex) =>
        partitionStateByVertex
          .collect { case (v, ps) if ps == Pending && Dag.ancestorsOf(v, edges).forall(partitionStateByVertex(_) == Complete) =>
            Dependency(p, Dag.neighborAncestorsOf(v, edges), v)
          }
      }.toSeq:_*
    )

  def updated(event: DagStateEvent)(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): DagState = event match {
    case StateInitializedEvent(partitionsByVertex) =>
      val rootVertex = Dag.root(edges)
      val rootVertexPartitions = partitionsByVertex(rootVertex)
      val vertexPartitions = partitionsByVertex.filterKeys(_ != rootVertex)
      require(
        vertexPartitions.forall(_._2.diff(rootVertexPartitions).isEmpty),
        alienPartitionsError(vertexPartitions, rootVertexPartitions)
      )
      def buildGraphForPartition(p: DagPartition, vertex: DagVertex): Map[DagVertex, String] = {
        def isComplete = (Dag.ancestorsOf(vertex, edges) + vertex).forall(partitionsByVertex.get(_).exists(_.contains(p)))
        val state = if (vertex == rootVertex || isComplete) Complete else Pending
        Dag.neighborDescendantsOf(vertex, edges).flatMap(buildGraphForPartition(p, _)).toMap + (vertex -> state)
      }
      DagState(
        TreeMap(rootVertexPartitions.map(p => p -> buildGraphForPartition(p, rootVertex)).toSeq:_*),
        Set.empty
      )

    case SaturationInitializedEvent(deps) =>
      def newStates =
        deps.foldLeft(vertexStatesByPartition) { case (acc, d@Dependency(p, sourceVertices, targetVertex)) =>
          acc.adjust(p) { partitionStateByVertex =>
              require(
                partitionStateByVertex(targetVertex) == Pending && sourceVertices.forall(partitionStateByVertex(_) == Complete),
                s"Illegal dag state of Pending2Progress $d\n: ${pprint.tokenize(acc(p)).mkString}"
              )
              partitionStateByVertex.updated(targetVertex, InProgress)
            }
        }
      copy(vertexStatesByPartition = newStates)

    case SaturationSucceededEvent(Dependency(p, sourceVertices, targetVertex)) =>
      val neighborDescendants = neighborDescendantsOf(targetVertex, edges)
      val partitionStateByVertex = vertexStatesByPartition(p)
      require(
        sourceVertices.forall(partitionStateByVertex(_) == Complete) && partitionStateByVertex(targetVertex) == InProgress && partitionStateByVertex.filterKeys(neighborDescendants.contains).forall(_._2 == Pending),
        s"Illegal inProgressToComplete state of $p from $sourceVertices to $targetVertex : ${partitionStateByVertex.mkString("\n","\n","\n")}"
      )
      copy(vertexStatesByPartition = vertexStatesByPartition.adjust(p)(_.updated(targetVertex, Complete)))

    case SaturationFailedEvent(Dependency(p, sourceVertices, targetV)) =>
      require(vertexStatesByPartition(p)(targetV) == InProgress, s"Illegal inProgressToFailed state of $p : ${vertexStatesByPartition(p).mkString("\n","\n","\n")}")
      copy(vertexStatesByPartition = vertexStatesByPartition - p, failedDependencies = failedDependencies + FailedDependency(p, sourceVertices, targetV))

    case PartitionCreatedEvent(p) =>
      val rootVertex = Dag.root(edges)
      val partitionStateByVertex = allVertices(edges).map(v => v -> (if (v == rootVertex) Complete else Pending)).toMap
      copy(vertexStatesByPartition = vertexStatesByPartition + (p -> partitionStateByVertex))

    case PartitionVertexRemovedEvent(p, vertex) =>
      val rootVertex = Dag.root(edges)
      val partitionStateByVertex = vertexStatesByPartition(p)
      require(vertex != rootVertex && partitionStateByVertex(vertex) == Complete, s"$p cannot be invalidated in root vertex or in vertex where it is not Complete : ${partitionStateByVertex.mkString("\n","\n","\n")}")
      require(ancestorsOf(vertex, edges).map(partitionStateByVertex).forall(_ == Complete), s"Completed $p in $vertex must have Complete ancestors : ${partitionStateByVertex.mkString("\n","\n","\n")}")
      val toBePendingVertices = descendantsOf(vertex, edges) + vertex
      copy(vertexStatesByPartition = vertexStatesByPartition.adjust(p)(partitionStateByVertex => partitionStateByVertex ++ toBePendingVertices.map(_ -> Pending)))
  }

}
