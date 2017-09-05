package gwi.saturator

import akka.actor.{ActorLogging, ActorRef, ActorRefFactory, Props}
import akka.persistence.fsm.PersistentFSM.{FSMState, LogEntry}
import akka.persistence.fsm.{LoggingPersistentFSM, PersistentFSM}
import gwi.saturator.DagFSM._
import gwi.saturator.DagState.DagStateEvent

import scala.math.Ordering
import scala.reflect.ClassTag

class DagFSM(init: () => List[(DagVertex, List[DagPartition])], handler: ActorRef)(implicit edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex])
  extends PersistentFSM[FSMState, DagState, DagStateEvent] with LoggingPersistentFSM[FSMState, DagState, DagStateEvent] with ActorLogging {
  import DagState._

  override def logDepth = 100
  override def persistenceId: String = self.path.name
  override def domainEventClassTag: ClassTag[DagStateEvent] = scala.reflect.classTag[DagStateEvent]

  startWith(DagEmpty, DagState.empty)

  when(DagEmpty) {
    case Event(Initialize(partitionsByVertex), _) =>
      goto(Saturating) applying (StateInitializedEvent(partitionsByVertex), SaturationInitializedEvent)
  }

  when(Saturating) {
    case Event(c@SaturationResponse(dep, succeeded), _) =>
      goto(Saturating) applying (DagStateEvent.forSaturationOutcome(succeeded, dep), SaturationInitializedEvent) replying Cmd.Submitted(c, stateName, stateData, getLog)

    case Event(c@CreatePartition(partition), _) =>
      goto(Saturating) applying (PartitionCreatedEvent(partition), SaturationInitializedEvent) replying Cmd.Submitted(c, stateName, stateData, getLog)

    case Event(c@RemovePartitionVertex(partition, vertex), _) =>
      goto(Saturating) applying (PartitionVertexRemovedEvent(partition, vertex), SaturationInitializedEvent) replying Cmd.Submitted(c, stateName, stateData, getLog)

    case Event(GetState, stateData) =>
      stay() replying Cmd.Submitted(GetState, stateName, stateData, getLog)

    case Event(ShutDown, _) =>
      stop() replying Cmd.Submitted(ShutDown, stateName, stateData, getLog)
  }

  onTransition {
    case DagEmpty -> DagEmpty if nextStateData.getVertexStatesByPartition.isEmpty => // Initialization happens only on fresh start, not when persistent event log is replayed
      log.info("DAG created, initializing ...")
      val initialData = init().map { case (v, p) => v -> p.toSet }.toMap
      self ! Initialize(initialData)
    case x -> Saturating =>
      val deps = nextStateData.getNewProgressingDeps
      if (x == DagEmpty) {
        log.info(s"Dag initialized ...")
        handler ! Cmd.Issued(Initialized, stateName, nextStateData, getLog)
      }
      if (deps.nonEmpty) {
        log.info(s"Saturating ${deps.size} dependencies ...")
        handler ! Cmd.Issued(Saturate(deps), stateName, nextStateData, getLog)
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
  sealed trait Cmd
  sealed trait Incoming extends Cmd
  sealed trait Outgoing extends Cmd
  sealed trait Internal extends Cmd

  protected[saturator] case class Initialize(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends Internal

  case class Saturate(dep: Set[Dependency]) extends Outgoing
  case object Initialized extends Outgoing

  case class CreatePartition(p: DagPartition) extends Incoming
  case class RemovePartitionVertex(p: DagPartition, v: DagVertex) extends Incoming
  case class SaturationResponse(dep: Dependency, succeeded: Boolean) extends Incoming
  case object GetState extends Incoming
  case object ShutDown extends Incoming

  protected[saturator] case object DagEmpty extends FSMState { override def identifier: String = "Empty" }
  protected[saturator] case object Saturating extends FSMState { override def identifier: String = "Saturating" }

  object Cmd {
    sealed trait CmdContainer[T <: Cmd] {
      def cmd: T
      def status: FSMState
      def state: DagState
      def log: IndexedSeq[LogEntry[FSMState, DagState]]
    }
    case class Submitted(cmd: Incoming, status: FSMState, state: DagState, log: IndexedSeq[LogEntry[FSMState, DagState]]) extends CmdContainer[Incoming]
    case class Issued(cmd: Outgoing, status: FSMState, state: DagState, log: IndexedSeq[LogEntry[FSMState, DagState]]) extends CmdContainer[Outgoing]
  }

  def apply(init: => List[(DagVertex, List[DagPartition])], handler: ActorRef, name: String)
           (implicit arf: ActorRefFactory, edges: Set[(DagVertex, DagVertex)], po: Ordering[DagPartition], vo: Ordering[DagVertex]): ActorRef = {
    arf.actorOf(Props(classOf[DagFSM], init _, handler, edges, po, vo), name)
  }
}
