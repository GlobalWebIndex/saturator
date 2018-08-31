package gwi.s8

import akka.actor.{ActorLogging, ActorRef, ActorRefFactory, Cancellable, Props}
import akka.persistence.Recovery
import akka.persistence.fsm.PersistentFSM.FSMState
import akka.persistence.fsm.{LoggingPersistentFSM, PersistentFSM}
import gwi.s8.impl.DagFSMState.DagStateEvent
import gwi.s8.impl.DagFSMState

import scala.math.Ordering
import scala.reflect.ClassTag

/** FSM Persistent actor that is propagating saturator state changes to the user system. */
class DagFSM(
        init: () => List[(DagVertex, List[DagPartition])],
        handler: ActorRef,
        schedule: Schedule
      )(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex])
  extends PersistentFSM[FSMState, DagFSMState, DagStateEvent] with LoggingPersistentFSM[FSMState, DagFSMState, DagStateEvent] with ActorLogging {
  import DagFSMState._
  import DagFSM._

  private var cancellables: List[Cancellable] = List()
  private var recoveryFlag = Recovery()
  override def recovery: Recovery = recoveryFlag

  override def logDepth = 100
  override def persistenceId: String = self.path.toStringWithoutAddress
  override def domainEventClassTag: ClassTag[DagStateEvent] = reflect.classTag[DagStateEvent]

  log.info(s"Starting DagFSM with persistence id $persistenceId ...")

  override def aroundPreStart(): Unit = {
    recoveryFlag = Recovery.none
    super.aroundPreStart()
  }

  private[this] def schedulePartitionCheck(currentState: DagFSMState): List[Cancellable] = {
    def scheduleCheck(checkOpt: Option[PartitionCheck], cmd: out.S8OutCmd): Option[Cancellable] =
      checkOpt.map { check =>
        log.info(s"Scheduling $cmd at $check for handler $handler")
        context.system.scheduler.schedule(
          check.delay, check.interval, self, system.Submit(cmd)
        )(context.dispatcher, handler)
      }

    val Schedule(createdCheckOpt, changedCheckOpt) = schedule
    List(
      scheduleCheck(createdCheckOpt, out.GetCreatedPartitions(currentState.getRoot)),
      scheduleCheck(changedCheckOpt, out.GetChangedPartitions(currentState.getRoot))
    ).flatten
  }

  startWith(DagEmpty, DagFSMState.empty)

  when(DagEmpty) {
    case Event(system.Initialize(partitionsByVertex), _) =>
      goto(Saturating) applying (StateInitializedEvent(partitionsByVertex), SaturationInitializedEvent)
  }

  when(Saturating) {
    case Event(c@in.AckSaturation(dep, succeeded), _) =>
      goto(Saturating)
        .applying(DagStateEvent.forSaturationOutcome(succeeded, dep), SaturationInitializedEvent)
        .replying(out.Submitted(c, stateData.partitionedDagState, stateData.depsInFlight))

    case Event(in.InsertPartitions(newPartitions), _) =>
      goto(Saturating)
        .applying(PartitionInsertsEvent(newPartitions), SaturationInitializedEvent)
        .replying(out.Submitted(in.InsertPartitions(newPartitions), stateData.partitionedDagState, stateData.depsInFlight))

    case Event(in.UpdatePartitions(updatedPartitions), _) =>
      goto(Saturating)
        .applying(PartitionUpdatesEvent(updatedPartitions), SaturationInitializedEvent)
        .replying(out.Submitted(in.UpdatePartitions(updatedPartitions), stateData.partitionedDagState, stateData.depsInFlight))

    case Event(in.RecreatePartition(partition, rootVertex), _) =>
      stay() replying out.Issued(out.GetRecreatedPartition(partition, rootVertex), stateData.partitionedDagState, stateData.depsInFlight)

    case Event(c@in.RedoDagBranch(partition, vertex), _) =>
      goto(Saturating)
        .applying (DagBranchRedoEvent(partition, vertex), SaturationInitializedEvent)
        .replying(out.Submitted(c, stateData.partitionedDagState, stateData.depsInFlight))

    case Event(c@in.FixPartition(partition), _) =>
      goto(Saturating)
        .applying(PartitionFixEvent(partition), SaturationInitializedEvent)
        .replying(out.Submitted(c, stateData.partitionedDagState, stateData.depsInFlight))

    case Event(in.GetState, stateData) =>
      stay() replying out.Submitted(in.GetState, stateData.partitionedDagState, stateData.depsInFlight)

    case Event(in.ShutDown, _) =>
      stop() replying out.Submitted(in.ShutDown, stateData.partitionedDagState, stateData.depsInFlight)

    case Event(system.Submit(cmd), _) =>
      log.info(s"Issuing cmd $cmd")
      stay() replying out.Issued(cmd, stateData.partitionedDagState, stateData.depsInFlight)
  }

  onTransition {
    case DagEmpty -> DagEmpty if nextStateData.isEmpty => // Initialization happens only on fresh start, not when persistent event log is replayed
      log.info("Starting FSM, DAG created, initializing ...")
      val initialData = init().map { case (v, p) => v -> p.toSet }.toMap
      self ! system.Initialize(initialData)
    case DagEmpty -> DagEmpty =>
      log.info("Starting FSM, DAG already exists ...")
    case x -> Saturating =>
      if (x == DagEmpty) {
        log.info(s"Dag initialized ...")
        handler ! out.Issued(out.Initialized, nextStateData.partitionedDagState, nextStateData.depsInFlight)
      }
      val deps = nextStateData.getNewProgressingDeps
      if (deps.nonEmpty) {
        log.info(s"Saturating ${deps.size} dependencies ...")
        deps.foreach( dep => handler ! out.Issued(out.Saturate(dep), nextStateData.partitionedDagState, nextStateData.depsInFlight) )
      } else if (nextStateData.isSaturated) {
        log.info(s"Dag is fully saturated ...")
        handler ! out.Issued(out.Saturated, nextStateData.partitionedDagState, nextStateData.depsInFlight)
      }
  }

  whenUnhandled {
    case Event(unknown, dagState) =>
      log.error(s"Unhandled command $unknown at status $stateName with state:\n $dagState")
      stay()
  }

  onTermination {
    case _ => cancellables.foreach(_.cancel())
  }

  def applyEvent(event: DagStateEvent, dagState: DagFSMState): DagFSMState = event match {
    case e =>
      dagState.updated(e)
  }

  override def onRecoveryCompleted() {
    cancellables = schedulePartitionCheck(stateData) ++ cancellables
  }
}

object DagFSM {
  protected[s8] case object DagEmpty extends FSMState { override def identifier: String = "Empty" }
  protected[s8] case object Saturating extends FSMState { override def identifier: String = "Saturating" }

  def apply(init: => List[(DagVertex, List[DagPartition])], handler: ActorRef, schedule: Schedule, name: String)
           (implicit arf: ActorRefFactory, edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): ActorRef = {
    arf.actorOf(Props(classOf[DagFSM], init _, handler, schedule, edges, po, vo), name)
  }
}
