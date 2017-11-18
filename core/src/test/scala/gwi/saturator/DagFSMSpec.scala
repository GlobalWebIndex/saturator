package gwi.saturator

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKitBase, TestProbe}
import akka.util.Timeout
import gwi.saturator.DagFSM._
import gwi.saturator.SaturatorCmd._
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers, Suite}

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class DagFSMSpec extends Suite with TestKitBase with BeforeAndAfterAll with DagTestSupport with Matchers with FreeSpecLike with ImplicitSender {
  private[this] implicit val timeout = Timeout(10.seconds)
  implicit lazy val system = ActorSystem("AkkaSuiteSystem")

  override def afterAll(): Unit = try Await.ready(Future(system.terminate())(ExecutionContext.global), Duration.Inf) finally super.afterAll()

  implicit val edges: Set[(DagVertex, DagVertex)] =
    Set(
      1 -> 2,
      1 -> 3,
      1 -> 4,
      1 -> 5,
      2 -> 6,
      3 -> 6,
      4 -> 7
    )

  private[this] def handleIssuedCmd(probe: TestProbe, fsmActor: ActorRef, failedDep: Option[Dependency], expectedDepOpt: Option[Dependency] = Option.empty): Dependency =
    probe.expectMsgType[Issued] match {
      case Issued(Saturate(dep),_,_,_) if expectedDepOpt.isEmpty || expectedDepOpt.contains(dep) =>
        fsmActor ! SaturationResponse(dep, !failedDep.contains(dep))
        expectMsgType[Submitted]
        dep
      case x =>
        sys.error(s"Unexpected message $x")
    }

  "testing one partition saturation thoroughly" in {

    def init: List[(Int, List[Long])] = List(1 -> List(1L))

    val probe = TestProbe()
    val fsmActor = DagFSM(init, probe.ref, None, "test-dag-fsm")
    assertResult(Initialized)(probe.expectMsgType[Issued].cmd)

    def assertSaturationOfDagForPartition(p: DagPartition) = {
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, Set(1), 2)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, Set(1), 3)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, Set(1), 4)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, Set(1), 5)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, Set(3,2), 6)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, Set(4), 7)))
      assertResult(Saturated)(probe.expectMsgType[Issued].cmd)
    }

    // initial saturation

    assertSaturationOfDagForPartition(1L)

    // saturation after adding new partition

    fsmActor ! PartitionInserts(TreeSet(2L))
    expectMsgType[Submitted]

    assertSaturationOfDagForPartition(2L)

    // saturation after changing existing partition

    fsmActor ! PartitionUpdates(TreeSet(2L))
    expectMsgType[Submitted]

    assertSaturationOfDagForPartition(2L)

    // saturation after redoing a dag branch

    fsmActor ! RedoDagBranch(2L, 2)
    expectMsgType[Submitted]

    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(2L, Set(1), 2)))
    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(2L, Set(3,2), 6)))
    assertResult(Saturated)(probe.expectMsgType[Issued].cmd)

    // saturation after recreating a partition

    fsmActor ! RedoDagBranch(2L, 1)
    expectMsgType[Submitted]

    assertSaturationOfDagForPartition(2L)

    // commands queuing

    (3L to 10L) foreach { p =>
      fsmActor ! PartitionInserts(TreeSet(p))
      expectMsgType[Submitted]
      assertSaturationOfDagForPartition(p)
    }

    // shutdown command

    fsmActor ! ShutDown
    expectMsgType[Submitted] match { case (Submitted(cmd, status, state, log)) =>
      assertResult(ShutDown)(cmd)
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 10)
      assert(state.isSaturated)
    }

    // persistent state replaying
    Thread.sleep(300)
    val newFsmActor = DagFSM(init, probe.ref, None, "test-dag-fsm")

    newFsmActor ! ShutDown
    expectMsgType[Submitted] match { case (Submitted(cmd, status, state, log)) =>
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 10)
      assert(state.isSaturated)
    }

  }

  "testing partition fixing" in {
    def init: List[(Int, List[Long])] = List(1 -> List(1L))
    val probe = TestProbe()
    val fsmActor = DagFSM(init, probe.ref, None, "dag-fsm-2")
    assertResult(Initialized)(probe.expectMsgType[Issued].cmd)

    def assertSaturationOfDagForPartition(p: DagPartition) = {
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, Set(1), 2)), Some(Dependency(p, Set(1), 2)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, Set(1), 2)), Some(Dependency(p, Set(1), 3)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, Set(1), 2)), Some(Dependency(p, Set(1), 4)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, Set(1), 2)), Some(Dependency(p, Set(1), 5)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, Set(4), 7)))
      assertResult(Saturated)(probe.expectMsgType[Issued].cmd)
    }

    assertSaturationOfDagForPartition(1L)

    fsmActor ! FixPartition(1L)
    expectMsgType[Submitted]

    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(1L, Set(1), 2)))
    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(1L, Set(3,2), 6)))
  }

  "testing multiple partition saturation roughly" in {
    def init: List[(Int, List[Long])] =
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
    val fsmActor = DagFSM(init, probe.ref, None, "dag-fsm-3")
    assertResult(Initialized)(probe.expectMsgType[Issued].cmd)

    val depsToSaturate =
      (1 to 30).map { _ =>
        handleIssuedCmd(probe, fsmActor, None)
      }

    // test partition order
    val partitionsToSaturate = depsToSaturate.map(_.p.pid.toInt).toList
    assertResult(partitionsToSaturate.sorted)(partitionsToSaturate)

    fsmActor ! ShutDown
    expectMsgType[Submitted] match { case (Submitted(cmd, status, state, log)) =>
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 9)
      assert(state.isSaturated)
    }

  }
}
