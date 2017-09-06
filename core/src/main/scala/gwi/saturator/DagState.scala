package gwi.saturator

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.math.Ordering

import collection.breakOut

protected[saturator] object DagState {
  implicit class TreeMapPimp[A, B](underlying: TreeMap[A, B]) {
    def adjust(k: A)(f: B => B) = underlying.updated(k, f(underlying(k)))
  }

  protected[saturator] def empty(implicit po: Ordering[DagPartition]): DagState =
    DagState(vertexStatesByPartition = TreeMap.empty[DagPartition, TreeMap[DagVertex, String]], depsInFlight = Set.empty[Dependency])
  protected[saturator] def initialized(vertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]]) =
    DagState(vertexStatesByPartition, Set.empty)

  protected[saturator] sealed trait DagStateEvent
  protected[saturator] case class StateInitializedEvent(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends DagStateEvent
  protected[saturator] case object SaturationInitializedEvent extends DagStateEvent
  protected[saturator] case class SaturationSucceededEvent(dep: Dependency) extends DagStateEvent
  protected[saturator] case class SaturationFailedEvent(dep: Dependency) extends DagStateEvent
  protected[saturator] case class PartitionCreatedEvent(p: DagPartition) extends DagStateEvent

  sealed trait AdhocEvent extends DagStateEvent
  protected[saturator] case class DagBranchRedoEvent(p: DagPartition, vertex: Option[DagVertex]) extends AdhocEvent
  protected[saturator] case class PartitionFixEvent(p: DagPartition) extends AdhocEvent
  protected[saturator] case class PartitionRecreatedEvent(p: DagPartition) extends AdhocEvent

  object DagStateEvent {
    protected[saturator] def forSaturationOutcome(succeeded: Boolean, dep: Dependency): DagStateEvent = if (succeeded) SaturationSucceededEvent(dep) else SaturationFailedEvent(dep)
  }
}

protected[saturator] case class DagState private(private val vertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]], private val depsInFlight: Set[Dependency]) extends LazyLogging {
  import Dag._
  import DagState._
  import DagVertex.State._

  private[this] def alienPartitionsError(vertexPartitions: Map[DagVertex, Set[DagPartition]], rootVertexPartitions: Set[DagPartition]) = {
    val (vertex, partitions) = vertexPartitions.find(_._2.diff(rootVertexPartitions).nonEmpty).map( t => t._1 -> t._2.diff(rootVertexPartitions)).get
    s"Graph contains $vertex with alien partitions:\n${partitions.mkString("\n","\n","\n")}" +
    s"Root partitions : ${rootVertexPartitions.mkString("\n","\n","\n")}Vertex partitions : ${vertexPartitions.find(_._2.diff(rootVertexPartitions).nonEmpty).map(_._2.mkString("\n","\n","\n"))}"
  }

  private[this] def getDepsByState(targetVertexState: String)(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): Set[Dependency] =
    TreeSet(
      vertexStatesByPartition.flatMap { case (p, partitionStateByVertex) =>
        partitionStateByVertex
          .collect { case (v, ps) if ps == targetVertexState && Dag.ancestorsOf(v, edges).forall(partitionStateByVertex(_) == Complete) =>
            Dependency(p, Dag.neighborAncestorsOf(v, edges), v)
          }
      }.toSeq:_*
    )

  private[this] def saturationSanityCheck(p: DagPartition, sourceVertices: Set[DagVertex], targetVertex: DagVertex)(implicit edges: Set[(DagVertex, DagVertex)]) = {
    val neighborDescendants = neighborDescendantsOf(targetVertex, edges)
    val partitionStateByVertex = vertexStatesByPartition(p)
    require(
      sourceVertices.forall(partitionStateByVertex(_) == Complete) && partitionStateByVertex(targetVertex) == InProgress && partitionStateByVertex.filterKeys(neighborDescendants.contains).forall(_._2 == Pending),
      s"Illegal inProgressToComplete state of $p from $sourceVertices to $targetVertex:\n${mkString(p).get}"
    )
  }

  def printable(implicit edges: Set[(DagVertex, DagVertex)]): Map[String, DagPartition.State] =
    vertexStatesByPartition.map { case (p, vertexStates) =>
      p.pid -> DagPartition.State(mkString(p).get, vertexStates.values)
    }(breakOut)

  def mkString(p: DagPartition)(implicit edges: Set[(DagVertex, DagVertex)]): Option[String] =
    vertexStatesByPartition.get(p).map { vertexStates =>
      val stateByVertex = vertexStates.map { case (v, state) => v.vid -> state }
      val edgesWithState: List[(String, String)] = edges.map(t => t._1.vid -> t._2.vid).map { case (f,t) => s"$f\n${stateByVertex(f)}" -> s"$t\n${stateByVertex(t)}" }(breakOut)
      val verticesWithState: Set[String] = stateByVertex.map { case (v, state) => s"$v\n$state"}(breakOut)
      GraphLayout.renderGraph(Graph(verticesWithState, edgesWithState))
    }

  def mkString(implicit edges: Set[(DagVertex, DagVertex)]): String = printable.map { case (p,graph) => s"-----------------$p-----------------\n${graph.serializedGraph}"}.mkString("\n","\n","\n")

  def getVertexStatesByPartition: Map[DagPartition, Map[DagVertex, String]] = vertexStatesByPartition
  def isSaturated(implicit edges: Set[(DagVertex, DagVertex)]): Boolean = vertexStatesByPartition.values.forall { vertexStates =>
    vertexStates(root(edges)) == Complete && descendantsOfRoot[DagVertex](edges, v => vertexStates(v) != Failed).forall(v => vertexStates(v) == Complete)
  }
  protected[saturator] def getVertexStatesFor(p: DagPartition): Map[DagVertex, String] = vertexStatesByPartition(p)
  protected[saturator] def getPendingDeps(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): Set[Dependency] = getDepsByState(Pending)
  protected[saturator] def getNewProgressingDeps(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): Set[Dependency] = getDepsByState(InProgress) -- depsInFlight

  protected[saturator] def updated(event: DagStateEvent)(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): DagState = event match {
    case StateInitializedEvent(partitionsByVertex) =>
      val rootVertex = Dag.root(edges)
      val rootVertexPartitions = partitionsByVertex(rootVertex)
      val vertexPartitions = partitionsByVertex.filterKeys(_ != rootVertex)
      if(!vertexPartitions.forall(_._2.diff(rootVertexPartitions).isEmpty))
        logger.warn(alienPartitionsError(vertexPartitions, rootVertexPartitions))

      def buildGraphForPartition(p: DagPartition, vertex: DagVertex): Map[DagVertex, String] = {
        def isComplete = (Dag.ancestorsOf(vertex, edges) + vertex).forall(partitionsByVertex.get(_).exists(_.contains(p)))
        val state = if (vertex == rootVertex || isComplete) Complete else Pending
        Dag.neighborDescendantsOf(vertex, edges).flatMap(buildGraphForPartition(p, _)).toMap + (vertex -> state)
      }
      DagState.initialized(TreeMap(rootVertexPartitions.map(p => p -> buildGraphForPartition(p, rootVertex)).toSeq:_*))

    case SaturationInitializedEvent =>
      def newStates =
        getPendingDeps.foldLeft(vertexStatesByPartition) { case (acc, d@Dependency(p, sourceVertices, targetVertex)) =>
          acc.adjust(p) { partitionStateByVertex =>
            require(
              partitionStateByVertex(targetVertex) == Pending && sourceVertices.forall(partitionStateByVertex(_) == Complete),
              s"Illegal dag state of Pending2Progress $d :\n${mkString(p).get}"
            )
            partitionStateByVertex.updated(targetVertex, InProgress)
          }
        }
      copy(vertexStatesByPartition = newStates, depsInFlight = depsInFlight ++ getNewProgressingDeps)

    case SaturationSucceededEvent(dep@Dependency(p, sourceVertices, targetVertex)) =>
      saturationSanityCheck(p, sourceVertices, targetVertex)
      val newState = copy(vertexStatesByPartition = vertexStatesByPartition.adjust(p)(_.updated(targetVertex, Complete)), depsInFlight = depsInFlight - dep)
      logger.info(s"Saturation of dependency succeeded : $dep\n${mkString(p).get}\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.mkString(p).get}")
      newState

    case SaturationFailedEvent(dep@Dependency(p, sourceVertices, targetVertex)) =>
      saturationSanityCheck(p, sourceVertices, targetVertex)
      val newState = copy(vertexStatesByPartition = vertexStatesByPartition.adjust(p)(_.updated(targetVertex, Failed)), depsInFlight = depsInFlight - dep)
      logger.info(s"Saturation of dependency failed : $dep\n${mkString(p).get}\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.mkString(p).get}")
      newState

    case PartitionCreatedEvent(p) =>
      val rootVertex = Dag.root(edges)
      val partitionStateByVertex = allVertices(edges).map(v => v -> (if (v == rootVertex) Complete else Pending)).toMap
      copy(vertexStatesByPartition = vertexStatesByPartition + (p -> partitionStateByVertex))

    case DagBranchRedoEvent(p, vertexOpt) =>
      val vertex = vertexOpt.getOrElse(Dag.root(edges))
      val partitionStateByVertex = vertexStatesByPartition(p)
      val toBePendingVertices = descendantsOf(vertex, edges) ++ vertexOpt
      require(partitionStateByVertex(vertex) == Complete, s"Dag branch cannot be redone at $p in vertex where it is not Complete :\n${mkString(p).get}")
      require(ancestorsOf(vertex, edges).map(partitionStateByVertex).forall(_ == Complete), s"Completed $vertex in $p must have Complete ancestors :\n${mkString(p).get}")
      require(toBePendingVertices.map(partitionStateByVertex).forall(_ == Complete), s"Completed $vertex in $p must have Complete descendants :\n${mkString(p).get}")
      val newState = copy(vertexStatesByPartition = vertexStatesByPartition.adjust(p)(partitionStateByVertex => partitionStateByVertex ++ toBePendingVertices.map(_ -> Pending)))
      logger.info(s"Dag branch from $vertex of partition $p is to be redone :\n${mkString(p).get}\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.mkString(p).get}")
      newState

    case PartitionFixEvent(p) =>
      val rootVertex = Dag.root(edges)
      val partitionStateByVertex = vertexStatesByPartition(p)
      val rootDescendants = descendantsOf(rootVertex, edges)
      require(!rootDescendants.map(partitionStateByVertex).contains(InProgress), s"Fixing progressing partition $p is not allowed :\n${mkString(p).get}")
      val toBePendingVertices = rootDescendants.filter(partitionStateByVertex(_) == Failed)
      val newState = copy(vertexStatesByPartition = vertexStatesByPartition.adjust(p)(partitionStateByVertex => partitionStateByVertex ++ toBePendingVertices.map(_ -> Pending)))
      logger.info(s"Partition $p is to be fixed :\n${mkString(p).get}\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.mkString(p).get}")
      newState

    case PartitionRecreatedEvent(p) =>
      val rootVertex = Dag.root(edges)
      val partitionStateByVertex = vertexStatesByPartition.getOrElse(p, throw new IllegalStateException(s"Recreating partition $p that doesn't exist !!!"))
      require(!descendantsOf(rootVertex, edges).map(partitionStateByVertex).contains(InProgress), s"Recreating progressing partition $p is not allowed :\n${mkString(p).get}")
      val newPartitionStateByVertex = allVertices(edges).map(v => v -> (if (v == rootVertex) Complete else Pending)).toMap
      copy(vertexStatesByPartition = vertexStatesByPartition + (p -> newPartitionStateByVertex))

  }

}
