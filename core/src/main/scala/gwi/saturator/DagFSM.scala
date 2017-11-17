package gwi.saturator

import akka.actor.{ActorLogging, ActorRef, ActorRefFactory, Props}
import akka.persistence.fsm.PersistentFSM.{FSMState, LogEntry}
import akka.persistence.fsm.{LoggingPersistentFSM, PersistentFSM}
import gwi.saturator.DagFSM._
import gwi.saturator.DagState.DagStateEvent
import gwi.saturator.SaturatorCmd._

import scala.concurrent.ExecutionContext.Implicits
import scala.concurrent.duration.FiniteDuration
import scala.math.Ordering
import scala.reflect.ClassTag

class DagFSM(
        init: () => List[(DagVertex, List[DagPartition])],
        handler: ActorRef,
        partitionChangesSchedule: Option[PartitionChangesSchedule]
      )(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex])
  extends PersistentFSM[FSMState, DagState, DagStateEvent] with LoggingPersistentFSM[FSMState, DagState, DagStateEvent] with ActorLogging {
  import DagState._
  import SaturatorCmd._

  override def logDepth = 100
  override def persistenceId: String = self.path.toStringWithoutAddress
  override def domainEventClassTag: ClassTag[DagStateEvent] = scala.reflect.classTag[DagStateEvent]

  private[this] def schedulePartitionCheck(currentState: DagState): Unit =
    partitionChangesSchedule.foreach { case PartitionChangesSchedule(interval, delay) =>
      log.info(s"Scheduling partition changes check ...")
      context.system.scheduler.schedule(interval, delay, handler, Issued(GetPartitionChanges(Dag.root(edges)), stateName, currentState, getLog))(Implicits.global, self)
    }

  startWith(DagEmpty, DagState.empty)

  when(DagEmpty) {
    case Event(Initialize(partitionsByVertex), _) =>
      goto(Saturating) applying (StateInitializedEvent(partitionsByVertex), SaturationInitializedEvent)
  }

  when(Saturating) {
    case Event(c@SaturationResponse(dep, succeeded), _) =>
      goto(Saturating) applying (DagStateEvent.forSaturationOutcome(succeeded, dep), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(c@PartitionInserts(newPartitions), _) =>
      goto(Saturating) applying (PartitionInsertsEvent(newPartitions), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(c@PartitionUpdates(updatedPartitions), _) =>
      goto(Saturating) applying (PartitionUpdatesEvent(updatedPartitions), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(c@RedoDagBranch(partition, vertex), _) =>
      goto(Saturating) applying (DagBranchRedoEvent(partition, vertex), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(c@FixPartition(partition), _) =>
      goto(Saturating) applying (PartitionFixEvent(partition), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(GetState, stateData) =>
      stay() replying Submitted(GetState, stateName, stateData, getLog)

    case Event(ShutDown, _) =>
      stop() replying Submitted(ShutDown, stateName, stateData, getLog)
  }

  onTransition {
    case DagEmpty -> DagEmpty if nextStateData.getVertexStatesByPartition.isEmpty => // Initialization happens only on fresh start, not when persistent event log is replayed
      log.info("Starting FSM, DAG created, initializing ...")
      val initialData = init().map { case (v, p) => v -> p.toSet }.toMap
      self ! Initialize(initialData)
      schedulePartitionCheck(nextStateData)
    case DagEmpty -> DagEmpty =>
      log.info("Starting FSM, DAG already exists ...")
      schedulePartitionCheck(nextStateData)
    case x -> Saturating =>
      if (x == DagEmpty) {
        log.info(s"Dag initialized ...")
        handler ! Issued(Initialized, stateName, nextStateData, getLog)
      }
      val deps = nextStateData.getNewProgressingDeps
      if (deps.nonEmpty) {
        log.info(s"Saturating ${deps.size} dependencies ...")
        handler ! Issued(Saturate(deps), stateName, nextStateData, getLog)
      } else if (nextStateData.isSaturated) {
        log.info(s"Dag is fully saturated ...")
        handler ! Issued(Saturated, stateName, nextStateData, getLog)
      }
  }

  whenUnhandled {
    case Event(unknown, dagState) =>
      log.error(s"Unhandled command $unknown at status $stateName with state:\n $dagState")
      stay()
  }

  def applyEvent(event: DagStateEvent, dagState: DagState): DagState = event match {
    case e =>
      dagState.updated(e)(edges,po,vo)
  }

}

object DagFSM {
  case class PartitionChangesSchedule(interval: FiniteDuration, delay: FiniteDuration)

  protected[saturator] case object DagEmpty extends FSMState { override def identifier: String = "Empty" }
  protected[saturator] case object Saturating extends FSMState { override def identifier: String = "Saturating" }

  sealed trait CmdContainer[T <: SaturatorCmd] {
    def cmd: T
    def status: FSMState
    def state: DagState
    def log: IndexedSeq[LogEntry[FSMState, DagState]]
  }
  case class Submitted(cmd: Incoming, status: FSMState, state: DagState, log: IndexedSeq[LogEntry[FSMState, DagState]]) extends CmdContainer[Incoming]
  case class Issued(cmd: Outgoing, status: FSMState, state: DagState, log: IndexedSeq[LogEntry[FSMState, DagState]]) extends CmdContainer[Outgoing]

  def apply(init: => List[(DagVertex, List[DagPartition])], handler: ActorRef, partitionChangesSchedule: Option[PartitionChangesSchedule], name: String)
           (implicit arf: ActorRefFactory, edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): ActorRef = {
    arf.actorOf(Props(classOf[DagFSM], init _, handler, partitionChangesSchedule, edges, po, vo), name)
  }
}
