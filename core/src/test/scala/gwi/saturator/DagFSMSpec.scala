package gwi.saturator

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import gwi.saturator.DagFSM._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FreeSpecLike, Matchers}
import redis.RedisClient

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object DagFSMSpec {
  val config = ConfigFactory.parseString(
    """
        akka {
          log-dead-letters-during-shutdown = off
          actor.warn-about-java-serializer-usage = off
          extensions = ["com.romix.akka.serialization.kryo.KryoSerializationExtension$"]
          akka-persistence-redis.journal.class = "com.hootsuite.akka.persistence.redis.journal.RedisJournal"
          persistence.journal.plugin = "akka-persistence-redis.journal"
        }
        redis {
          host = localhost
          port = 6379
          password = foo
          sentinel = false
        }
        """.stripMargin
  ).withFallback(ConfigFactory.load())
}

class DagFSMSpec(_system: ActorSystem) extends TestKit(_system) with DockerSupport with Matchers with FreeSpecLike with BeforeAndAfterAll with BeforeAndAfterEach with ImplicitSender {
  import DagMock._
  def this() = this(ActorSystem("DagFSMSpec", DagFSMSpec.config))

  private[this] var redisClient: RedisClient = null

  override def beforeAll(): Unit = try super.beforeAll() finally {
    startContainer("redis", "redis-test", 6379)(())
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

  "testing one partition saturation thoroughly" in {

    def init: List[(Int, List[Long])] = List(1 -> List(1L))

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

    val probe = TestProbe()
    val fsmActor = DagFSM(init, probe.ref, "test-dag-fsm")
    assertResult(Initialized)(probe.expectMsgType[Cmd.Issued].cmd)

    def handleIssuedCmd(expectedDeps: Set[Dependency]): Unit =
      probe.expectMsgType[Cmd.Issued] match {
        case Cmd.Issued(Saturate(deps),_,_,_) if deps == expectedDeps =>
          deps.foreach { dep =>
            fsmActor ! SaturationResponse(dep, true)
            expectMsgType[Cmd.Submitted]
          }
        case x =>
          sys.error(s"Unexpected message $x")
      }


    def assertSaturationOfDagForPartition(p: DagPartition) = {
      handleIssuedCmd(
        TreeSet(
          Dependency(p, Set(1), 2),
          Dependency(p, Set(1), 3),
          Dependency(p, Set(1), 4),
          Dependency(p, Set(1), 5)
        )
      )
      handleIssuedCmd(TreeSet(Dependency(p, Set(3,2), 6)))
      handleIssuedCmd(TreeSet(Dependency(p, Set(4), 7)))
    }

    // initial saturation

    assertSaturationOfDagForPartition(1L)

    // saturation after adding new partition

    fsmActor ! CreatePartition(2L)
    expectMsgType[Cmd.Submitted]

    assertSaturationOfDagForPartition(2L)

    // saturation after removing a partition vertex

    fsmActor ! RedoDagBranch(2L, 2)
    expectMsgType[Cmd.Submitted]

    handleIssuedCmd(TreeSet(Dependency(2L, Set(1), 2)))
    handleIssuedCmd(TreeSet(Dependency(2L, Set(3,2), 6)))

    // commands queuing

    (3L to 10L) foreach { p =>
      fsmActor ! CreatePartition(p)
      expectMsgType[Cmd.Submitted]
      assertSaturationOfDagForPartition(p)
    }

    // shutdown command

    fsmActor ! ShutDown
    expectMsgType[Cmd.Submitted] match { case (Cmd.Submitted(cmd, status, state, log)) =>
      assertResult(ShutDown)(cmd)
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 10)
      assert(state.isSaturated)
    }

    // persistent state replaying
    Thread.sleep(300)
    val newFsmActor = DagFSM(init, probe.ref, "test-dag-fsm")

    newFsmActor ! ShutDown
    expectMsgType[Cmd.Submitted] match { case (Cmd.Submitted(cmd, status, state, log)) =>
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 10)
      assert(state.isSaturated)
    }

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

    val probe = TestProbe()
    val fsmActor = DagFSM(init, probe.ref, "dag-fsm-2")
    assertResult(Initialized)(probe.expectMsgType[Cmd.Issued].cmd)

    def handleIssuedCmd(): Unit =
      probe.expectMsgType[Cmd.Issued] match {
        case Cmd.Issued(Saturate(deps),_,_,_) =>
          deps.foreach { dep =>
            fsmActor ! SaturationResponse(dep, true)
            expectMsgType[Cmd.Submitted]
          }
        case x =>
          sys.error(s"Unexpected message $x")
      }

    handleIssuedCmd()
    handleIssuedCmd()
    handleIssuedCmd()
    handleIssuedCmd()
    handleIssuedCmd()
    handleIssuedCmd()
    handleIssuedCmd()

    fsmActor ! ShutDown
    expectMsgType[Cmd.Submitted] match { case (Cmd.Submitted(cmd, status, state, log)) =>
      assertResult(Saturating)(status)
      assert(state.getVertexStatesByPartition.size == 9)
      assert(state.isSaturated)
    }

  }
}