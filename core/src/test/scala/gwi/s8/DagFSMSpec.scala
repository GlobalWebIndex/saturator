package gwi.s8

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKitBase, TestProbe}
import gwi.s8.DagFSM._
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers, Suite}

import collection.immutable.TreeSet
import concurrent.duration._
import concurrent.{Await, ExecutionContext, Future}

class DagFSMSpec extends Suite with TestKitBase with BeforeAndAfterAll with DagTestSupport with Matchers with FreeSpecLike with ImplicitSender {
  implicit lazy val system = ActorSystem("AkkaSuiteSystem")

  override def afterAll(): Unit = try Await.ready(Future(system.terminate())(ExecutionContext.global), Duration.Inf) finally super.afterAll()

  implicit val edges: Set[(DagVertex, DagVertex)] =
    Set(
      1 -> 2, 2 -> 6,
      1 -> 3, 3 -> 6,
      1 -> 4, 4 -> 7,
      1 -> 5
    )

  private[this] def handleIssuedCmd(probe: TestProbe, fsmActor: ActorRef, failedDep: Option[Dependency], expectedDepOpt: Option[Dependency] = Option.empty): Dependency =
    probe.expectMsgType[Issued] match {
      case Issued(out.Saturate(dep),_,_,_) if expectedDepOpt.isEmpty || expectedDepOpt.contains(dep) =>
        fsmActor ! in.AckSaturation(dep, !failedDep.contains(dep))
        expectMsgType[Submitted]
        dep
      case x =>
        sys.error(s"Unexpected message $x")
    }

  "testing one partition saturation thoroughly" in {

    def partitionsByVertex: List[(Int, List[Long])] = List(1 -> List(1L))

    val probe = TestProbe()
    val fsmActor = DagFSM(partitionsByVertex, probe.ref, None, "test-dag-fsm")
    assertResult(out.Initialized)(probe.expectMsgType[Issued].cmd)

    def assertSaturationOfDagForPartition(p: DagPartition) = {
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(1), 2)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(1), 3)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(1), 4)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(1), 5)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(3,2), 6)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(4), 7)))
      assertResult(out.Saturated)(probe.expectMsgType[Issued].cmd)
    }

    // initial saturation

    assertSaturationOfDagForPartition(1L)

    // saturation after adding new partition

    fsmActor ! in.InsertPartitions(TreeSet(2L))
    expectMsgType[Submitted]

    assertSaturationOfDagForPartition(2L)

    // saturation after changing existing partition

    fsmActor ! in.UpdatePartitions(TreeSet(2L))
    expectMsgType[Submitted]

    assertSaturationOfDagForPartition(2L)

    // saturation after redoing a dag branch

    fsmActor ! in.RedoDagBranch(2L, 2)
    expectMsgType[Submitted]

    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(2L, TreeSet(1), 2)))
    handleIssuedCmd(probe, fsmActor, None, Some(Dependency(2L, TreeSet(3,2), 6)))
    assertResult(out.Saturated)(probe.expectMsgType[Issued].cmd)

    // saturation after recreating a partition

    fsmActor ! in.RedoDagBranch(2L, 1)
    expectMsgType[Submitted]

    assertSaturationOfDagForPartition(2L)

    // commands queuing

    (3L to 10L) foreach { p =>
      fsmActor ! in.InsertPartitions(TreeSet(p))
      expectMsgType[Submitted]
      assertSaturationOfDagForPartition(p)
    }

    // shutdown command

    fsmActor ! in.ShutDown
    expectMsgType[Submitted] match { case (Submitted(cmd, status, state, log)) =>
      assertResult(in.ShutDown)(cmd)
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 10)
      assert(state.isSaturated)
    }

    // persistent state replaying
    Thread.sleep(300)
    val newFsmActor = DagFSM(partitionsByVertex, probe.ref, None, "test-dag-fsm")

    newFsmActor ! in.ShutDown
    expectMsgType[Submitted] match { case (Submitted(cmd, status, state, log)) =>
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 10)
      assert(state.isSaturated)
    }

  }

  "testing partition fixing" in {
    def partitionsByVertex: List[(Int, List[Long])] = List(1 -> List(1L))
    val probe = TestProbe()
    val fsmActor = DagFSM(partitionsByVertex, probe.ref, None, "dag-fsm-2")
    assertResult(out.Initialized)(probe.expectMsgType[Issued].cmd)

    def assertSaturationOfDagForPartition(p: DagPartition) = {
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, TreeSet(1), 2)), Some(Dependency(p, TreeSet(1), 2)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, TreeSet(1), 2)), Some(Dependency(p, TreeSet(1), 3)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, TreeSet(1), 2)), Some(Dependency(p, TreeSet(1), 4)))
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, TreeSet(1), 2)), Some(Dependency(p, TreeSet(1), 5)))
      handleIssuedCmd(probe, fsmActor, None, Some(Dependency(p, TreeSet(4), 7)))
      assertResult(out.Saturated)(probe.expectMsgType[Issued].cmd)
    }

    assertSaturationOfDagForPartition(1L)

    fsmActor ! in.FixPartition(1L)
    expectMsgType[Submitted]

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
    val fsmActor = DagFSM(partitionsByVertex, probe.ref, None, "dag-fsm-4")
    assertResult(out.Initialized)(probe.expectMsgType[Issued].cmd)

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
    expectMsgType[Submitted] match { case (Submitted(cmd, status, state, log)) =>
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 9)
      assert(state.isSaturated)
    }

  }
}
