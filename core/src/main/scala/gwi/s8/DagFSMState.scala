package gwi.s8

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.math.Ordering

protected[s8] case class DagFSMState(partitionedDagState: PartitionedDagState, depsInFlight: Set[Dependency])

protected[s8] object DagFSMState extends StrictLogging {

  protected[s8] sealed trait DagStateEvent
  protected[s8] case class StateInitializedEvent(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends DagStateEvent
  protected[s8] case object SaturationInitializedEvent extends DagStateEvent
  protected[s8] case class SaturationSucceededEvent(dep: Dependency) extends DagStateEvent
  protected[s8] case class SaturationFailedEvent(dep: Dependency) extends DagStateEvent
  protected[s8] case class PartitionInsertsEvent(partitions: TreeSet[DagPartition]) extends DagStateEvent
  protected[s8] case class PartitionUpdatesEvent(partitions: TreeSet[DagPartition]) extends DagStateEvent

  protected[s8] sealed trait AdhocEvent extends DagStateEvent
  protected[s8] case class DagBranchRedoEvent(p: DagPartition, vertex: DagVertex) extends AdhocEvent
  protected[s8] case class PartitionFixEvent(p: DagPartition) extends AdhocEvent

  object DagStateEvent {
    protected[s8] def forSaturationOutcome(succeeded: Boolean, dep: Dependency): DagStateEvent = if (succeeded) SaturationSucceededEvent(dep) else SaturationFailedEvent(dep)
  }

  protected[s8] def empty(implicit po: Ordering[DagPartition]): DagFSMState =
    DagFSMState(partitionedDagState = TreeMap.empty[DagPartition, PartitionState], depsInFlight = Set.empty[Dependency])
  protected[s8] def initialized(partitionedDagState: PartitionedDagState) =
    DagFSMState(partitionedDagState, Set.empty)

  implicit class DagFSMStatePimp(underlying: DagFSMState)(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]) extends LazyLogging {
    import PartitionedDagState._
    private[this] implicit val dag = Dag(edges)

    protected[s8] def getRoot: DagVertex = dag.root
    protected[s8] def getNewProgressingDeps: Set[Dependency] = underlying.partitionedDagState.getProgressingDeps -- underlying.depsInFlight
    protected[s8] def getPendingDeps: Set[Dependency] = underlying.partitionedDagState.getPendingDeps
    protected[s8] def isSaturated: Boolean = underlying.partitionedDagState.isSaturated
    protected[s8] def isEmpty: Boolean = underlying.partitionedDagState.isEmpty

    protected[s8] def updated(event: DagStateEvent): DagFSMState = event match {
      case StateInitializedEvent(partitionsByVertex) =>
        DagFSMState.initialized(PartitionedDagState(partitionsByVertex))
      case SaturationInitializedEvent =>
        underlying.copy(partitionedDagState = underlying.partitionedDagState.progress, depsInFlight = underlying.depsInFlight ++ underlying.partitionedDagState.getProgressingDeps)
      case SaturationSucceededEvent(dep) =>
        underlying.copy(partitionedDagState = underlying.partitionedDagState.succeed(dep), depsInFlight = underlying.depsInFlight - dep)
      case SaturationFailedEvent(dep) =>
        underlying.copy(partitionedDagState = underlying.partitionedDagState.fail(dep), depsInFlight = underlying.depsInFlight - dep)
      case PartitionInsertsEvent(newPartitions) =>
        underlying.copy(partitionedDagState = underlying.partitionedDagState.addPartitions(newPartitions))
      case PartitionUpdatesEvent(updatedPartitions) =>
        underlying.copy(partitionedDagState = underlying.partitionedDagState.updatePartitions(updatedPartitions))
      case DagBranchRedoEvent(p, vertex) =>
        underlying.copy(partitionedDagState = underlying.partitionedDagState.redo(p, vertex))
      case PartitionFixEvent(p) =>
        underlying.copy(partitionedDagState = underlying.partitionedDagState.fix(p))
    }

  }
}