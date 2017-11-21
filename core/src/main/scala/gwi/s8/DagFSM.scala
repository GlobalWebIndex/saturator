package gwi.s8

import akka.actor.{ActorLogging, ActorRef, ActorRefFactory, Cancellable, Props}
import akka.persistence.fsm.PersistentFSM.FSMState
import akka.persistence.fsm.{LoggingPersistentFSM, PersistentFSM}
import gwi.s8.DagState.DagStateEvent

import scala.concurrent.ExecutionContext.Implicits
import scala.math.Ordering
import scala.reflect.ClassTag

class DagFSM(
        init: () => List[(DagVertex, List[DagPartition])],
        handler: ActorRef,
        schedule: Schedule
      )(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex])
  extends PersistentFSM[FSMState, DagState, DagStateEvent] with LoggingPersistentFSM[FSMState, DagState, DagStateEvent] with ActorLogging {
  import DagState._
  import DagFSM._

  override def logDepth = 100
  override def persistenceId: String = self.path.toStringWithoutAddress
  override def domainEventClassTag: ClassTag[DagStateEvent] = reflect.classTag[DagStateEvent]

  log.info(s"Starting DagFSM with persistence id $persistenceId ...")

  private[this] def schedulePartitionCheck(currentState: DagState): List[Cancellable] = {
    def scheduleCheck(checkOpt: Option[PartitionCheck], cmd: out.S8OutCmd): Option[Cancellable] =
      checkOpt.map { check =>
        log.info(s"Scheduling partitions check : $cmd")
        context.system.scheduler.schedule(
          check.interval, check.delay, handler, out.Issued(cmd, currentState.vertexStatesByPartition, currentState.depsInFlight)
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
      goto(Saturating)
        .applying(DagStateEvent.forSaturationOutcome(succeeded, dep), SaturationInitializedEvent)
        .replying(in.Submitted(c, stateData.vertexStatesByPartition, stateData.depsInFlight))

    case Event(c@in.InsertPartitions(newPartitions), _) =>
      goto(Saturating)
        .applying(PartitionInsertsEvent(newPartitions), SaturationInitializedEvent)
        .replying(in.Submitted(c, stateData.vertexStatesByPartition, stateData.depsInFlight))

    case Event(c@in.UpdatePartitions(updatedPartitions), _) =>
      goto(Saturating)
        .applying(PartitionUpdatesEvent(updatedPartitions), SaturationInitializedEvent)
        .replying(in.Submitted(c, stateData.vertexStatesByPartition, stateData.depsInFlight))

    case Event(c@in.RedoDagBranch(partition, vertex), _) =>
      goto(Saturating)
        .applying (DagBranchRedoEvent(partition, vertex), SaturationInitializedEvent)
        .replying(in.Submitted(c, stateData.vertexStatesByPartition, stateData.depsInFlight))

    case Event(c@in.FixPartition(partition), _) =>
      goto(Saturating)
        .applying(PartitionFixEvent(partition), SaturationInitializedEvent)
        .replying(in.Submitted(c, stateData.vertexStatesByPartition, stateData.depsInFlight))

    case Event(in.GetState, stateData) =>
      stay() replying in.Submitted(in.GetState, stateData.vertexStatesByPartition, stateData.depsInFlight)

    case Event(in.ShutDown, _) =>
      stop() replying in.Submitted(in.ShutDown, stateData.vertexStatesByPartition, stateData.depsInFlight)
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
        handler ! out.Issued(out.Initialized, nextStateData.vertexStatesByPartition, nextStateData.depsInFlight)
      }
      val deps = nextStateData.getNewProgressingDeps
      if (deps.nonEmpty) {
        log.info(s"Saturating ${deps.size} dependencies ...")
        deps.foreach( dep => handler ! out.Issued(out.Saturate(dep), nextStateData.vertexStatesByPartition, nextStateData.depsInFlight) )
      } else if (nextStateData.isSaturated) {
        log.info(s"Dag is fully saturated ...")
        handler ! out.Issued(out.Saturated, nextStateData.vertexStatesByPartition, nextStateData.depsInFlight)
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

  def apply(init: => List[(DagVertex, List[DagPartition])], handler: ActorRef, schedule: Schedule, name: String)
           (implicit arf: ActorRefFactory, edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): ActorRef = {
    arf.actorOf(Props(classOf[DagFSM], init _, handler, schedule, edges, po, vo), name)
  }
}
