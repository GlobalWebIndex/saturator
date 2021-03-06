package gwi.s8

import akka.actor.{Actor, ActorRef, ActorSystem, Kill, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKitBase, TestProbe}
import com.typesafe.scalalogging.StrictLogging
import gwi.s8.Supervisor.CreateDagFsm
import gwi.s8.in.GetState
import gwi.s8.out.Submitted
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers, Suite}

import collection.immutable.{TreeMap, TreeSet}
import concurrent.duration._
import concurrent.{Await, ExecutionContext, Future}

class DagFSMSpec extends Suite with TestKitBase with BeforeAndAfterAll with DagTestSupport with Matchers with FreeSpecLike with ImplicitSender {
  implicit lazy val system = ActorSystem("AkkaSuiteSystem")

  type State = TreeMap[DagPartition, Map[DagVertex, String]]

  override def afterAll(): Unit = try Await.ready(Future(system.terminate())(ExecutionContext.global), Duration.Inf) finally super.afterAll()

  private[this] implicit val edges: Set[(DagVertex, DagVertex)] =
    Set(
      1 -> 2, 2 -> 6,
      1 -> 3, 3 -> 6,
      1 -> 4, 4 -> 7,
      1 -> 5
    )

  private[this] def handleIssuedCmd(probe: TestProbe, fsmActor: ActorRef, failedDep: Option[Dependency], expectedDepOpt: Option[Dependency] = Option.empty): Dependency =
    probe.expectMsgType[out.Issued] match {
      case out.Issued(out.Saturate(dep),_,_) if expectedDepOpt.isEmpty || expectedDepOpt.contains(dep) =>
        fsmActor ! in.AckSaturation(dep, !failedDep.contains(dep))
        expectMsgType[out.Submitted]
        dep
      case x =>
        sys.error(s"Unexpected message $x")
    }

  "testing one partition saturation thoroughly" in {

    def partitionsByVertex: List[(Int, List[Long])] = List(1 -> List(1L))

    val probe = TestProbe()
    val fsmActor = DagFSM(partitionsByVertex, probe.ref, Schedule.noop, "test-dag-fsm")
    assertResult(out.Initialized)(probe.expectMsgType[out.Issued].msg)

    def assertSaturationOfDagForPartition(p: DagPartition) = {
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(1), 2)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(1), 3)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(1), 4)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(1), 5)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(3,2), 6)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(4), 7)))
      assertResult(out.Saturated)(probe.expectMsgType[out.Issued].msg)
    }

    // initial saturation

    assertSaturationOfDagForPartition(1L)

    // saturation after adding new partition

    fsmActor ! in.InsertPartitions(TreeSet(2L))
    expectMsgType[out.Submitted]

    assertSaturationOfDagForPartition(2L)

    // saturation after changing existing partition

    fsmActor ! in.UpdatePartitions(TreeSet(2L))
    expectMsgType[out.Submitted]

    assertSaturationOfDagForPartition(2L)

    // saturation after redoing a dag branch

    fsmActor ! in.RedoDagBranch(2L, 2)
    expectMsgType[out.Submitted]

    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(2L, TreeSet(1), 2)))
    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(2L, TreeSet(3,2), 6)))
    assertResult(out.Saturated)(probe.expectMsgType[out.Issued].msg)

    // saturation after recreating a partition

    fsmActor ! in.RedoDagBranch(2L, 1)
    expectMsgType[out.Submitted]

    assertSaturationOfDagForPartition(2L)

    // commands queuing

    (3L to 10L) foreach { p =>
      fsmActor ! in.InsertPartitions(TreeSet(p))
      expectMsgType[out.Submitted]
      assertSaturationOfDagForPartition(p)
    }

    // shutdown command

    fsmActor ! in.ShutDown
    expectMsgType[out.Submitted] match { case (out.Submitted(cmd, vertexStatesByPartition, depsInFlight)) =>
      assertResult(in.ShutDown)(cmd)
      assert(vertexStatesByPartition.size == 10)
      assert(depsInFlight.isEmpty)
    }
  }

  "testing partition fixing" in {
    def partitionsByVertex: List[(Int, List[Long])] = List(1 -> List(1L))
    val probe = TestProbe()
    val fsmActor = DagFSM(partitionsByVertex, probe.ref, Schedule.noop, "dag-fsm-2")
    assertResult(out.Initialized)(probe.expectMsgType[out.Issued].msg)

    def assertSaturationOfDagForPartition(p: DagPartition) = {
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, TreeSet(1), 2)), Some(Dependency(p, TreeSet(1), 2)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, TreeSet(1), 2)), Some(Dependency(p, TreeSet(1), 3)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, TreeSet(1), 2)), Some(Dependency(p, TreeSet(1), 4)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, TreeSet(1), 2)), Some(Dependency(p, TreeSet(1), 5)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(4), 7)))
      assertResult(out.Saturated)(probe.expectMsgType[out.Issued].msg)
    }

    assertSaturationOfDagForPartition(1L)

    fsmActor ! in.FixPartition(1L)
    expectMsgType[out.Submitted]

    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(1L, TreeSet(1), 2)))
    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(1L, TreeSet(3,2), 6)))
  }

  "testing multiple partition saturation roughly" in {
    val partitionsByVertex: List[(Int, List[Long])] =
      List(
        1 -> (1L to 9L).toList,
        2 -> (1L to 7L).toList,
        3 -> (1L to 7L).toList,
        4 -> (1L to 5L).toList,
        5 -> (1L to 3L).toList,
        6 -> List(1L),
        7 -> List(1L)
      )

    val probe = TestProbe()
    val fsmActor = DagFSM(partitionsByVertex, probe.ref, Schedule.noop, "dag-fsm-4")
    assertResult(out.Initialized)(probe.expectMsgType[out.Issued].msg)

    val depsToSaturate = (1 to 30).map ( _ => handleIssuedCmd(probe, fsmActor, None) )

    // test partition ordering - absolute order of partition saturation would be ineffective because partition out of order could be executed in the mean time
    depsToSaturate.groupBy(_.sourceVertices.head.vid).values.foreach { depsWithSameSourceVertex =>
      val headVertexSaturationPartitions = depsWithSameSourceVertex.map(_.p.pid.toInt)
      assertResult(headVertexSaturationPartitions.sorted)(headVertexSaturationPartitions)
    }
    // test vertex ordering - vertices within partition should be always executed in order
    depsToSaturate.groupBy(_.p.pid).values.foreach { partitionDeps =>
      val headVertexSaturationPartitions = partitionDeps.map(_.sourceVertices.head.vid.toInt)
      assertResult(headVertexSaturationPartitions.sorted)(headVertexSaturationPartitions)
    }

    fsmActor ! in.ShutDown
    expectMsgType[out.Submitted] match { case (out.Submitted(cmd, vertexStatesByPartition, depsInFlight)) =>
      assert(vertexStatesByPartition.size == 9)
      assert(depsInFlight.isEmpty)
    }

  }

  "created partition check should be initialized after the actor is started" in {
    val actorName = "dag-fsm-start-created-partiton-check"
    val probe = TestProbe()
    val partitionsByVertex = List(1 -> List(1L))
    val schedule = Schedule(Some(CreatedPartitionCheck(1.minute, Duration.Zero)), None)

    DagFSM(partitionsByVertex, probe.ref, schedule, actorName)

    probe.fishForSpecificMessage(5.seconds) {
      case out.Issued(out.GetCreatedPartitions(_),_,_) => true
    }
  }

  "created partition check should be initialized after the actor is restarted" in {
    val actorName = "dag-fsm-restart-created-partiton-check"
    val probe = TestProbe()
    val partitionsByVertex = List(1 -> List(1L))
    val schedule = Schedule(Some(CreatedPartitionCheck(1.minute, Duration.Zero)), None)

    val fsmActor = DagFSM(partitionsByVertex, probe.ref, Schedule.noop, actorName)

    fsmActor ! PoisonPill
    Thread.sleep(1000)

    DagFSM(partitionsByVertex, probe.ref, schedule, actorName)

    probe.fishForSpecificMessage(5.seconds) {
      case out.Issued(out.GetCreatedPartitions(_),_,_) => true
    }
  }

  "changed partition check should be initialized after the actor is started" in {
    val actorName = "dag-fsm-start-changed-partiton-check"
    val probe = TestProbe()
    val partitionsByVertex = List(1 -> List(1L))
    val schedule = Schedule(None, Some(ChangedPartitionCheck(1.minute, Duration.Zero)))

    DagFSM(partitionsByVertex, probe.ref, schedule, actorName)

    probe.fishForSpecificMessage(5.seconds) {
      case out.Issued(out.GetChangedPartitions(_),_,_) => true
    }
  }

  "changed partition check should be initialized after the actor is restarted" in {
    val actorName = "dag-fsm-restart-changed-partiton-check"
    val probe = TestProbe()
    val partitionsByVertex = List(1 -> List(1L))
    val schedule = Schedule(None, Some(ChangedPartitionCheck(1.minute, Duration.Zero)))

    val fsmActor = DagFSM(partitionsByVertex, probe.ref, Schedule.noop, actorName)

    fsmActor ! PoisonPill
    Thread.sleep(1000)

    DagFSM(partitionsByVertex, probe.ref, schedule, actorName)

    probe.fishForSpecificMessage(5.seconds) {
      case out.Issued(out.GetChangedPartitions(_),_,_) => true
    }
  }

  "DagFSM should replay state from event store when it crashed" in {
    implicit val edges: Set[(DagVertex, DagVertex)] = Set(1 -> 2)
    val partitionsByVertex: List[(Int, List[Long])] = List(1 -> List(1))
    val probe = TestProbe()
    val supervisor = system.actorOf(Props[Supervisor], "supervisor")
    supervisor ! CreateDagFsm(partitionsByVertex, edges, probe.ref, "fsm-test")
    val fsmActor = expectMsgType[ActorRef]

    assertResult(out.Initialized)(probe.expectMsgType[out.Issued].msg)

    val initState = getState(fsmActor)

    fsmActor ! in.InsertPartitions(TreeSet(2))
    expectMsgType[out.Submitted]

    val updatedState = getState(fsmActor)

    fsmActor ! Kill

    val newInitState = getState(fsmActor)

    system.stop(supervisor)

    assertResult(updatedState)(newInitState)
  }

  "should use current state when it is initialized" in {
    val edges: Set[(DagVertex, DagVertex)] = Set(1 -> 2)
    val partitionsByVertex: List[(Int, List[Long])] = List(1 -> List(1))
    val probe = TestProbe()
    val supervisor = system.actorOf(Props[Supervisor], "supervisor")
    supervisor ! CreateDagFsm(partitionsByVertex, edges, probe.ref, "fsm-test")
    val fsmActor = expectMsgType[ActorRef]
    assertResult(out.Initialized)(probe.expectMsgType[out.Issued].msg)

    val initState = getState(fsmActor)

    fsmActor ! in.InsertPartitions(TreeSet(2))
    expectMsgType[out.Submitted]

    val updatedState = getState(fsmActor)

    system.stop(fsmActor)

    val newProbe = TestProbe()
    val newPartitionsByVertex: List[(Int, List[Long])] = List(1 -> List(4))
    supervisor ! CreateDagFsm(newPartitionsByVertex, edges, newProbe.ref, "fsm-test")
    val newFsmActor = expectMsgType[ActorRef]
    assertResult(out.Initialized)(newProbe.expectMsgType[out.Issued].msg)

    val newInitState = getState(newFsmActor)

    system.stop(supervisor)
    Thread.sleep(1000)

    assertResult(1)(newInitState.size)
    assertResult(DagPartition("4"))(newInitState.firstKey)
  }

  private def getState(fsmActor: ActorRef): State = {
    fsmActor ! GetState
    fishForSpecificMessage(5.seconds) {
      case Submitted(GetState, state, _) => state
    }
  }
}

object Supervisor {
  case class CreateDagFsm(
    init: List[(DagVertex, List[DagPartition])],
    edges: Set[(DagVertex, DagVertex)],
    handler: ActorRef,
    name: String
  )
}

class Supervisor extends Actor with DagTestSupport with StrictLogging {
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _: Exception => Restart
    }

  def receive = {
    case m: CreateDagFsm =>
      implicit val edges = m.edges
      sender() ! DagFSM(m.init, m.handler, Schedule.noop, m.name)
    case m => logger.error(s"Unknown message $m")
  }
}
