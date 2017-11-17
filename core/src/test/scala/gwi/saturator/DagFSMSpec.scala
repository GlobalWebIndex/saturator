package gwi.saturator

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import gwi.saturator.DagFSM._
import gwi.saturator.SaturatorCmd._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FreeSpecLike, Matchers}
import redis.RedisClient

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

abstract class AbstractDagFSMSpec(_system: ActorSystem) extends TestKit(_system) with DockerSupport with Matchers with FreeSpecLike with BeforeAndAfterAll with BeforeAndAfterEach with ImplicitSender {
  import DagMock._

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

  def handleIssuedCmd(probe: TestProbe, fsmActor: ActorRef, failedDep: Option[Dependency], expectedDepsOpt: Option[Set[Dependency]] = Option.empty): Unit =
    probe.expectMsgType[Issued] match {
      case Issued(Saturate(deps),_,_,_) if expectedDepsOpt.isEmpty || expectedDepsOpt.contains(deps) =>
        deps.foreach { dep =>
          fsmActor ! SaturationResponse(dep, !failedDep.contains(dep))
          expectMsgType[Submitted]
        }
      case x =>
        sys.error(s"Unexpected message $x")
    }

  "testing one partition saturation thoroughly" in {

    def init: List[(Int, List[Long])] = List(1 -> List(1L))

    val probe = TestProbe()
    val fsmActor = DagFSM(init, probe.ref, None, "test-dag-fsm")
    assertResult(Initialized)(probe.expectMsgType[Issued].cmd)

    def assertSaturationOfDagForPartition(p: DagPartition) = {
      handleIssuedCmd(probe, fsmActor, None,
        Some(
          TreeSet(
            Dependency(p, Set(1), 2),
            Dependency(p, Set(1), 3),
            Dependency(p, Set(1), 4),
            Dependency(p, Set(1), 5)
          )
        )
      )
      handleIssuedCmd(probe, fsmActor, None, Some(TreeSet(Dependency(p, Set(3,2), 6))))
      handleIssuedCmd(probe, fsmActor, None, Some(TreeSet(Dependency(p, Set(4), 7))))
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

    handleIssuedCmd(probe, fsmActor, None, Some(TreeSet(Dependency(2L, Set(1), 2))))
    handleIssuedCmd(probe, fsmActor, None, Some(TreeSet(Dependency(2L, Set(3,2), 6))))
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
      handleIssuedCmd(probe, fsmActor, Some(Dependency(p, Set(1), 2)),
        Some(
          TreeSet(
            Dependency(p, Set(1), 2),
            Dependency(p, Set(1), 3),
            Dependency(p, Set(1), 4),
            Dependency(p, Set(1), 5)
          )
        )
      )
      handleIssuedCmd(probe, fsmActor, None, Some(TreeSet(Dependency(p, Set(4), 7))))
      assertResult(Saturated)(probe.expectMsgType[Issued].cmd)
    }

    assertSaturationOfDagForPartition(1L)

    fsmActor ! FixPartition(1L)
    expectMsgType[Submitted]

    handleIssuedCmd(probe, fsmActor, None, Some(TreeSet(Dependency(1L, Set(1), 2))))
    handleIssuedCmd(probe, fsmActor, None, Some(TreeSet(Dependency(1L, Set(3,2), 6))))
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

    handleIssuedCmd(probe, fsmActor, None)
    handleIssuedCmd(probe, fsmActor, None)
    handleIssuedCmd(probe, fsmActor, None)
    handleIssuedCmd(probe, fsmActor, None)
    handleIssuedCmd(probe, fsmActor, None)
    handleIssuedCmd(probe, fsmActor, None)
    handleIssuedCmd(probe, fsmActor, None)

    fsmActor ! ShutDown
    expectMsgType[Submitted] match { case (Submitted(cmd, status, state, log)) =>
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 9)
      assert(state.isSaturated)
    }

  }
}

class DynamoDagFSMSpec(_system: ActorSystem) extends AbstractDagFSMSpec(_system: ActorSystem) {
  def this() = this(ActorSystem("DynamoDagFSMSpec", ConfigFactory.load("dynamo").withFallback(ConfigFactory.parseResources("reference.conf")).resolve()))

  private[this] val dynamoInit =
    """
      aws \
       --endpoint-url=http://localhost:8000 dynamodb create-table \
       --table-name akka-persistence \
       --attribute-definitions \
           AttributeName=par,AttributeType=S \
           AttributeName=num,AttributeType=N \
       --key-schema AttributeName=par,KeyType=HASH AttributeName=num,KeyType=RANGE \
       --provisioned-throughput ReadCapacityUnits=10000,WriteCapacityUnits=10000
    """.stripMargin

  override def beforeAll(): Unit = try super.beforeAll() finally {
    startContainer("dwmkerr/dynamodb", "dynamo-test", Seq(PPorts.from(8000,8000)), Some("-sharedDb")) {
      startContainer("garland/aws-cli-docker", "dynamo-init", Seq.empty, Some(dynamoInit))(())
    }
    Thread.sleep(1000)
  }

  override def afterAll(): Unit = try {
    stopContainer("dynamo-test") {
      stopContainer("dynamo-init")(())
    }
    Await.ready(Future(system.terminate())(ExecutionContext.global), Duration.Inf)
  } finally super.afterAll()
}

class RedisDagFSMSpec(_system: ActorSystem) extends AbstractDagFSMSpec(_system: ActorSystem) {
  def this() = this(ActorSystem("RedisDagFSMSpec", ConfigFactory.load("redis").withFallback(ConfigFactory.parseResources("reference.conf")).resolve()))

  private[this] var redisClient: RedisClient = null

  override def beforeAll(): Unit = try super.beforeAll() finally {
    startContainer("redis", "redis-test", Seq(PPorts.from(6379,6379)), None)(())
    Thread.sleep(1000)
    redisClient = RedisClient("localhost", 6379, name = "test-redis-client")
  }

  override def afterEach(): Unit = try {
    Await.ready(redisClient.flushall(), 2.seconds)
  } finally super.afterAll()

  override def afterAll(): Unit = try {
    stopContainer("redis-test")(())
    Await.ready(Future(system.terminate())(ExecutionContext.global), Duration.Inf)
  } finally super.afterAll()
}
