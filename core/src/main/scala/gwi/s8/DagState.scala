package gwi.s8

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.typesafe.scalalogging.LazyLogging

import collection.immutable.{TreeMap, TreeSet}
import math.Ordering
import collection.{Iterable, breakOut}

protected[s8] object DagState {
  implicit class TreeMapPimp[K, V](underlying: TreeMap[K, V]) {
    def adjust(k: K)(f: V => V): TreeMap[K, V] = underlying.updated(k, f(underlying(k)))
    def adjustOpt(k: K)(f: Option[V] => V): TreeMap[K, V] = underlying updated(k, f(underlying.get(k)))
    def adjustFlatOpt(k: K)(f: Option[V] => Option[V]): TreeMap[K, V] = f(underlying.get(k)) match {
      case None =>
        underlying
      case Some(v) =>
        underlying updated(k, v)
    }
  }

  protected[s8] def empty(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): DagState =
    DagState(vertexStatesByPartition = TreeMap.empty[DagPartition, TreeMap[DagVertex, String]], depsInFlight = Set.empty[Dependency])
  protected[s8] def initialized(vertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]])
                               (implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]) =
    DagState(vertexStatesByPartition, Set.empty)

  protected[s8] sealed trait DagStateEvent
  protected[s8] case class StateInitializedEvent(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends DagStateEvent
  protected[s8] case object SaturationInitializedEvent extends DagStateEvent
  protected[s8] case class SaturationSucceededEvent(dep: Dependency) extends DagStateEvent
  protected[s8] case class SaturationFailedEvent(dep: Dependency) extends DagStateEvent
  protected[s8] case class PartitionInsertsEvent(partitions: TreeSet[DagPartition]) extends DagStateEvent
  protected[s8] case class PartitionUpdatesEvent(partitions: TreeSet[DagPartition]) extends DagStateEvent

  sealed trait AdhocEvent extends DagStateEvent
  protected[s8] case class DagBranchRedoEvent(p: DagPartition, vertex: DagVertex) extends AdhocEvent
  protected[s8] case class PartitionFixEvent(p: DagPartition) extends AdhocEvent

  object DagStateEvent {
    protected[s8] def forSaturationOutcome(succeeded: Boolean, dep: Dependency): DagStateEvent = if (succeeded) SaturationSucceededEvent(dep) else SaturationFailedEvent(dep)
  }
}

case class DagState private(vertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]], depsInFlight: Set[Dependency])
                           (implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]) extends PrintableSupport with LazyLogging {
  import DagState._
  import DagVertex.State._

  protected val dag = Dag(edges)

  private[this] def alienPartitionsError(vertexPartitions: Map[DagVertex, Set[DagPartition]], rootVertexPartitions: Set[DagPartition]) = {
    val (vertex, partitions) = vertexPartitions.find(_._2.diff(rootVertexPartitions).nonEmpty).map( t => t._1 -> t._2.diff(rootVertexPartitions)).get
    s"Graph contains $vertex with alien partitions:\n${partitions.mkString("\n","\n","\n")}" +
    s"Root partitions : ${rootVertexPartitions.mkString("\n","\n","\n")}Vertex partitions : ${vertexPartitions.find(_._2.diff(rootVertexPartitions).nonEmpty).map(_._2.mkString("\n","\n","\n"))}"
  }

  private[this] def getDepsByState(targetVertexState: String): Set[Dependency] =
    TreeSet(
      vertexStatesByPartition.flatMap { case (p, partitionStateByVertex) =>
        partitionStateByVertex
          .collect { case (v, ps) if ps == targetVertexState && dag.ancestorsOf(v).forall(partitionStateByVertex(_) == Complete) =>
            Dependency(p, dag.neighborAncestorsOf(v), v)
          }
      }.toSeq:_*
    )

  private[this] def saturationSanityCheck(p: DagPartition, sourceVertices: Set[DagVertex], targetVertex: DagVertex): Unit = {
    val neighborDescendants = dag.neighborDescendantsOf(targetVertex)
    val partitionStateByVertex = vertexStatesByPartition(p)
    require(
      sourceVertices.forall(partitionStateByVertex(_) == Complete) && partitionStateByVertex(targetVertex) == InProgress && partitionStateByVertex.filterKeys(neighborDescendants.contains).forall(_._2 == Pending),
      s"Illegal inProgressToComplete state of $p from $sourceVertices to $targetVertex:\n${mkString(p).get}"
    )
  }

  private[this] def redoPartitionBranch(p: DagPartition, vertex: DagVertex, partitionStateByVertex: Map[DagVertex, String]): Option[Map[DagVertex, String]] = {
    val branch = dag.descendantsOf(vertex)() ++ Option(vertex).filter(_ != dag.root)
    val branchStates = branch.map(partitionStateByVertex)
    val ancestorStates = dag.ancestorsOf(vertex).map(partitionStateByVertex)
    if (branchStates.contains(InProgress)) {
      logger.warn(s"Redoing dag branch of $vertex with progressing partition $p is not allowed :\n${mkString(p).get}")
      None
    } else if (!ancestorStates.forall(_ == Complete)){
      logger.warn(s"Dag branch cannot be redone at $p in vertex $vertex from incomplete ancestors :\n${mkString(p).get}")
      None
    } else
      Some(partitionStateByVertex ++ branch.map(_ -> Pending))
  }

  def getRoot: DagVertex = dag.root
  def getVertexStatesByPartition: TreeMap[DagPartition, Map[DagVertex, String]] = vertexStatesByPartition
  def isSaturated: Boolean = vertexStatesByPartition.values.forall { vertexStates =>
    vertexStates(dag.root) == Complete && dag.descendantsOfRoot(v => vertexStates(v) != Failed).forall(v => vertexStates(v) == Complete)
  }
  protected[s8] def getVertexStatesFor(p: DagPartition): Map[DagVertex, String] = vertexStatesByPartition(p)
  protected[s8] def getPendingDeps: Set[Dependency] = getDepsByState(Pending)
  protected[s8] def getNewProgressingDeps: Set[Dependency] = getDepsByState(InProgress) -- depsInFlight

  protected[s8] def updated(event: DagStateEvent): DagState = event match {
    case StateInitializedEvent(partitionsByVertex) =>
      val rootVertex = dag.root
      val rootVertexPartitions = partitionsByVertex(rootVertex)
      val vertexPartitions = partitionsByVertex.filterKeys(_ != rootVertex)
      if(!vertexPartitions.forall(_._2.diff(rootVertexPartitions).isEmpty))
        logger.warn(alienPartitionsError(vertexPartitions, rootVertexPartitions))

      def buildGraphForPartition(p: DagPartition, vertex: DagVertex): Map[DagVertex, String] = {
        def isComplete = (dag.ancestorsOf(vertex) + vertex).forall(partitionsByVertex.get(_).exists(_.contains(p)))
        val state = if (vertex == rootVertex || isComplete) Complete else Pending
        dag.neighborDescendantsOf(vertex).flatMap(buildGraphForPartition(p, _)).toMap + (vertex -> state)
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

    case PartitionInsertsEvent(partitions) =>
      val rootVertex = dag.root
      val partitionStateByVertex = dag.allVertices.map(v => v -> (if (v == rootVertex) Complete else Pending)).toMap
      logger.info(s"Partitions created : ${partitions.mkString("\n","\n","\n")}")
      copy(vertexStatesByPartition = vertexStatesByPartition ++ partitions.map(_ -> partitionStateByVertex))

    case PartitionUpdatesEvent(partitions) =>
      val rootVertex = dag.root
      partitions.filter(vertexStatesByPartition.contains).foreach( p => logger.error(s"Partition $p cannot be changed because it is missing !!!") )
      val changedStatesByPartition =
        partitions
          .collect { case p if vertexStatesByPartition.contains(p) => p -> vertexStatesByPartition(p) }
          .flatMap { case (p, states) => redoPartitionBranch(p, rootVertex, states).map(p -> _) }
      logger.info(s"Partitions changed : ${partitions.mkString("\n","\n","\n")}")
      copy(vertexStatesByPartition = vertexStatesByPartition ++ changedStatesByPartition)

    case DagBranchRedoEvent(p, vertex) =>
      val newPartitionStateByVertex =
        vertexStatesByPartition.adjustFlatOpt(p) {
          case None =>
            logger.error(s"Redoing $vertex of partition $p that doesn't exist !!!")
            None
          case Some(partitionStateByVertex) =>
            redoPartitionBranch(p, vertex, partitionStateByVertex)
        }
      val newState = copy(vertexStatesByPartition = newPartitionStateByVertex)
      logger.info(s"Dag branch from $vertex of partition $p is to be redone :\n${mkString(p).get}\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.mkString(p).get}")
      newState

    case PartitionFixEvent(p) =>
      val rootVertex = dag.root
      val partitionStateByVertex = vertexStatesByPartition(p)
      val rootDescendants = dag.descendantsOf(rootVertex)()
      require(!rootDescendants.map(partitionStateByVertex).contains(InProgress), s"Fixing progressing partition $p is not allowed :\n${mkString(p).get}")
      val toBePendingVertices = rootDescendants.filter(partitionStateByVertex(_) == Failed)
      val newState = copy(vertexStatesByPartition = vertexStatesByPartition.adjust(p)(partitionStateByVertex => partitionStateByVertex ++ toBePendingVertices.map(_ -> Pending)))
      logger.info(s"Partition $p is to be fixed :\n${mkString(p).get}\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.mkString(p).get}")
      newState

  }

}

sealed trait PrintableSupport { this: DagState =>
  def printable: Map[String, PrintableState] =
    vertexStatesByPartition.map { case (p, vertexStates) =>
      p.pid -> PrintableState(mkString(p).get, vertexStates.values)
    }(breakOut)

  def mkString(p: DagPartition): Option[String] =
    vertexStatesByPartition.get(p).map { vertexStates =>
      val stateByVertex = vertexStates.map { case (v, state) => v.vid -> state }
      val edgesWithState: List[(String, String)] = dag.edges.map(t => t._1.vid -> t._2.vid).map { case (f,t) => s"$f\n${stateByVertex(f)}" -> s"$t\n${stateByVertex(t)}" }(breakOut)
      val verticesWithState: Set[String] = stateByVertex.map { case (v, state) => s"$v\n$state"}(breakOut)
      GraphLayout.renderGraph(Graph(verticesWithState, edgesWithState))
    }

  def mkString: String = printable.map { case (p,graph) => s"-----------------$p-----------------\n${graph.serializedGraph}"}.mkString("\n","\n","\n")
}

sealed trait PrintableState {
  def serializedGraph: String
}

object PrintableState {
  case class Progressing(serializedGraph: String) extends PrintableState
  case class Complete(serializedGraph: String) extends PrintableState
  case class Failed(serializedGraph: String) extends PrintableState
  def apply(serializedGraph: String, states: Iterable[String]): PrintableState = states.toSet match {
    case xs if xs.size == 1 && xs.head == DagVertex.State.Complete => Complete(serializedGraph)
    case xs if xs.contains(DagVertex.State.Failed) => Failed(serializedGraph)
    case _ => Progressing(serializedGraph)
  }
}
