package gwi.saturator

import akka.actor.Status.Failure
import akka.actor.{ActorLogging, ActorRef, ActorRefFactory, Props}
import akka.persistence.fsm.PersistentFSM.{FSMState, LogEntry}
import akka.persistence.fsm.{LoggingPersistentFSM, PersistentFSM}
import gwi.saturator.DagState.DagStateEvent

import scala.math.Ordering
import scala.reflect.ClassTag

sealed trait Status extends FSMState
object Status {
  val Empty = "DagEmpty"
  val DagSaturated = "DagSaturated"
  val DagUnSaturated = "DagUnSaturated"
}

class DagFSM(edges: Set[(DagVertex, DagVertex)], init: () => List[(DagVertex, List[DagPartition])], handler: ActorRef)(implicit po: Ordering[DagPartition], vo: Ordering[DagVertex])
  extends PersistentFSM[Status, DagState, DagStateEvent] with LoggingPersistentFSM[Status, DagState, DagStateEvent] with ActorLogging {
  import DagFSM._
  import DagState._

  override def logDepth = 100
  override def persistenceId: String = self.path.name
  override def domainEventClassTag: ClassTag[DagStateEvent] = scala.reflect.classTag[DagStateEvent]

  private var cmdQueue = Vector.empty[(Cmd, ActorRef)] // non-persistent user command queue, commands issued after ShutDown command would get refused

  startWith(DagEmpty, DagState.empty)

  when(DagEmpty) {
    case Event(Initialize(partitionsByVertex), _) =>
      goto(DagUnSaturated) applying StateInitializedEvent(partitionsByVertex)
  }

  when(DagUnSaturated) {
    case Event(Saturate(deps), _) =>
      if (deps.isEmpty) {
        log.info("No dependencies to saturate ...")
        goto(DagSaturated)
      } else {
        stay() applying SaturationInitializedEvent(deps) andThen(_ => handler ! Saturate(deps) )
      }
    case Event(SaturationResponse(dep, succeeded), dagState) =>
      val event = if (succeeded) SaturationSucceededEvent(dep) else SaturationFailedEvent(dep)
      val newState = dagState.updated(event)(edges,po,vo)
      lazy val newDeps = newState.getPendingToProgressPartitions(edges)
      if (newState.isSaturated)
        goto(DagSaturated) applying event
      else if (newDeps.nonEmpty)
        stay().applying(event, SaturationInitializedEvent(newDeps)) andThen (_ => handler ! Saturate(newDeps) )
      else
        stay() applying event
  }

  when(DagSaturated) {
    case Event(c@CreatePartition(partition), _) =>
      goto(DagUnSaturated) applying PartitionCreatedEvent(partition) andThen(dagState => sender() ! Cmd.Submitted(c, stateName, dagState, getLog) )
    case Event(c@RemovePartitionVertex(partition, vertex), _) =>
      goto(DagUnSaturated) applying PartitionVertexRemovedEvent(partition, vertex) andThen(dagState => sender() ! Cmd.Submitted(c, stateName, dagState, getLog) )
    case Event(ShutDown, _) =>
      stop() replying Cmd.Submitted(ShutDown, stateName, stateData, getLog)
  }

  whenUnhandled {
    case Event(ShutDown, dagState) =>
      cmdQueue :+= (ShutDown -> sender())
      stay()
    case Event(c@(_:CreatePartition | _:RemovePartitionVertex), _) =>
      if (cmdQueue.lastOption.exists(_._1 == ShutDown))
        sender() ! Failure(AlreadyShutdownException("Shutdown was scheduled, please try later !!!"))
      else
        cmdQueue :+= (c.asInstanceOf[Cmd] -> sender())
      stay()
    case Event(unknown, dagState) =>
      log.warning(s"Unhandled command $unknown at status $stateName with state:\n $dagState")
      stay()
  }

  onTransition {
    case DagEmpty -> DagEmpty if nextStateData.getVertexStatesByPartition.isEmpty =>
      log.info("DAG is empty, initializing  ...")
      val initialData = init().map { case (v, p) => v -> p.toSet }.toMap
      self ! Initialize(initialData)
    case DagSaturated -> DagUnSaturated =>
      log.info("DAG unsaturated, getting dependencies to saturate ...")
      val p2ps = nextStateData.getPendingToProgressPartitions(edges)
      self ! Saturate(p2ps)
    case DagEmpty -> DagUnSaturated =>
      log.info("DAG initialized, getting dependencies to saturate ...")
      val p2ps = nextStateData.getPendingToProgressPartitions(edges)
      self ! Saturate(p2ps)
    case DagUnSaturated -> DagSaturated =>
      log.info("Dag Saturated ...")
      cmdQueue.headOption.foreach { case (cmd, originalSender) =>
        log.info("Enqueuing {}", cmd)
        self.tell(cmd, originalSender)
        cmdQueue = cmdQueue.tail
      }
  }

  onTermination {
    case StopEvent(reason, status, dagState) =>
      cmdQueue.foreach { case (cmd, originalSender) => originalSender ! Cmd.Rejected(cmd, status, dagState, getLog) }
      reason match {
        case PersistentFSM.Failure(ex) =>
          log.error(s"Failure due to $ex\n at status {} with state \n{}\n with events:\n{}", status, dagState, pprint.tokenize(getLog).mkString)
        case PersistentFSM.Shutdown =>
          log.warning("Outside termination at status {} with state \n{}\n with events:\n{}", status, dagState, pprint.tokenize(getLog.map(_.stateName)).mkString)
        case PersistentFSM.Normal =>
          log.info("Stopping at status {} with state \n{}\n with events:\n{}", status, status, pprint.tokenize(getLog.map(_.stateName)).mkString)
      }
  }

  def applyEvent(event: DagStateEvent, dagState: DagState): DagState = event match {
    case e =>
      dagState.updated(e)(edges,po,vo)
  }

}

object DagFSM {
  sealed trait Cmd
  case class CreatePartition(p: DagPartition) extends Cmd
  case class RemovePartitionVertex(p: DagPartition, v: DagVertex) extends Cmd
  case class Saturate(dep: Set[Dependency]) extends Cmd
  case class SaturationResponse(dep: Dependency, succeeded: Boolean) extends Cmd
  case object ShutDown extends Cmd
  private case class Initialize(partitionsByVertex: Map[DagVertex, Set[DagPartition]]) extends Cmd

  private case object DagEmpty extends Status { override def identifier: String = "DagEmpty" }
  private case object DagSaturated extends Status { override def identifier: String = "DagSaturated" }
  private case object DagUnSaturated extends Status { override def identifier: String = "DagUnSaturated" }

  object Cmd {
    case class Submitted(cmd: Cmd, status: Status, state: DagState, log: IndexedSeq[LogEntry[Status, DagState]])
    case class Rejected(cmd: Cmd, status: Status, state: DagState, log: IndexedSeq[LogEntry[Status, DagState]])
  }

  case class AlreadyShutdownException(msg: String) extends Exception(msg)

  def apply(edges: Set[(DagVertex, DagVertex)], init: => List[(DagVertex, List[DagPartition])], handler: ActorRef, name: String = "dag-fsm")
           (implicit arf: ActorRefFactory, po: Ordering[DagPartition], vo: Ordering[DagVertex]): ActorRef = {
    arf.actorOf(Props(classOf[DagFSM], edges, init _, handler, po, vo), name)
  }
}


