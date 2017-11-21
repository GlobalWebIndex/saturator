package gwi.s8

import akka.actor.{ActorLogging, ActorRef, ActorRefFactory, Cancellable, Props}
import akka.persistence.fsm.PersistentFSM.{FSMState, LogEntry}
import akka.persistence.fsm.{LoggingPersistentFSM, PersistentFSM}
import gwi.s8.DagFSM._
import gwi.s8.DagState.DagStateEvent

import concurrent.ExecutionContext.Implicits
import math.Ordering
import reflect.ClassTag

class DagFSM(
        init: () => List[(DagVertex, List[DagPartition])],
        handler: ActorRef,
        schedule: Schedule
      )(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex])
  extends PersistentFSM[FSMState, DagState, DagStateEvent] with LoggingPersistentFSM[FSMState, DagState, DagStateEvent] with ActorLogging {
  import DagState._

  override def logDepth = 100
  override def persistenceId: String = self.path.toStringWithoutAddress
  override def domainEventClassTag: ClassTag[DagStateEvent] = reflect.classTag[DagStateEvent]

  log.info(s"Starting DagFSM with persistence id $persistenceId ...")

  private[this] def schedulePartitionCheck(currentState: DagState): List[Cancellable] = {
    def scheduleCheck(checkOpt: Option[PartitionCheck], cmd: out.S8OutCmd): Option[Cancellable] =
      checkOpt.map { check =>
        log.info(s"Scheduling partitions check : $cmd")
        context.system.scheduler.schedule(
          check.interval, check.delay, handler, Issued(cmd, stateName, currentState, getLog)
        )(Implicits.global, self)
      }

    val Schedule(createdCheckOpt, changedCheckOpt) = schedule
    List(
      scheduleCheck(createdCheckOpt, out.GetCreatedPartitions(currentState.getRoot)),
      scheduleCheck(changedCheckOpt, out.GetChangedPartitions(currentState.getRoot))
    ).flatten
  }

  startWith(DagEmpty, DagState.empty)

  when(DagEmpty) {
    case Event(Initialize(partitionsByVertex), _) =>
      goto(Saturating) applying (StateInitializedEvent(partitionsByVertex), SaturationInitializedEvent)
  }

  when(Saturating) {
    case Event(c@in.AckSaturation(dep, succeeded), _) =>
      goto(Saturating) applying (DagStateEvent.forSaturationOutcome(succeeded, dep), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(c@in.InsertPartitions(newPartitions), _) =>
      goto(Saturating) applying (PartitionInsertsEvent(newPartitions), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(c@in.UpdatePartitions(updatedPartitions), _) =>
      goto(Saturating) applying (PartitionUpdatesEvent(updatedPartitions), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(c@in.RedoDagBranch(partition, vertex), _) =>
      goto(Saturating) applying (DagBranchRedoEvent(partition, vertex), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(c@in.FixPartition(partition), _) =>
      goto(Saturating) applying (PartitionFixEvent(partition), SaturationInitializedEvent) replying Submitted(c, stateName, stateData, getLog)

    case Event(in.GetState, stateData) =>
      stay() replying Submitted(in.GetState, stateName, stateData, getLog)

    case Event(in.ShutDown, _) =>
      stop() replying Submitted(in.ShutDown, stateName, stateData, getLog)
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
        handler ! Issued(out.Initialized, stateName, nextStateData, getLog)
      }
      val deps = nextStateData.getNewProgressingDeps
      if (deps.nonEmpty) {
        log.info(s"Saturating ${deps.size} dependencies ...")
        deps.foreach( dep => handler ! Issued(out.Saturate(dep), stateName, nextStateData, getLog) )
      } else if (nextStateData.isSaturated) {
        log.info(s"Dag is fully saturated ...")
        handler ! Issued(out.Saturated, stateName, nextStateData, getLog)
      }
  }

  whenUnhandled {
    case Event(unknown, dagState) =>
      log.error(s"Unhandled command $unknown at status $stateName with state:\n $dagState")
      stay()
  }

  def applyEvent(event: DagStateEvent, dagState: DagState): DagState = event match {
    case e =>
      dagState.updated(e)
  }

}

object DagFSM {
  protected[s8] case object DagEmpty extends FSMState { override def identifier: String = "Empty" }
  protected[s8] case object Saturating extends FSMState { override def identifier: String = "Saturating" }

  sealed trait CmdContainer[T <: S8Msg] {
    def cmd: T
    def status: FSMState
    def state: DagState
    def log: IndexedSeq[LogEntry[FSMState, DagState]]
  }
  case class Submitted(cmd: in.S8IncomingMsg, status: FSMState, state: DagState, log: IndexedSeq[LogEntry[FSMState, DagState]]) extends CmdContainer[in.S8IncomingMsg]
  case class Issued(cmd: out.S8OutMsg, status: FSMState, state: DagState, log: IndexedSeq[LogEntry[FSMState, DagState]]) extends CmdContainer[out.S8OutMsg]

  def apply(init: => List[(DagVertex, List[DagPartition])], handler: ActorRef, schedule: Schedule, name: String)
           (implicit arf: ActorRefFactory, edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): ActorRef = {
    arf.actorOf(Props(classOf[DagFSM], init _, handler, schedule, edges, po, vo), name)
  }
}
