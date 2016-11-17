package gwi.saturator

import gwi.saturator.DagState._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FreeSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.language.implicitConversions

class DagStateSpec extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  import DagMock._
  import DagVertex.State._
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

  "loading DAG" - {
    "should fail in case of alien partition presence" in {
      val partitionsByVertex = Map[Int, Set[Long]](
        1 -> Set(1L, 2L),
        2 -> Set(1L, 2L),
        3 -> Set(1L, 2L, 3L)
      )
      intercept[IllegalArgumentException](DagState.empty.updated(StateInitializedEvent(partitionsByVertex)))
    }

    "should load complete DAG" in {
      val singlePartitionPresentInAllVertices: Map[DagVertex, Set[DagPartition]] = (1 to 7).map(_ -> Set(1L)).toMap
      val expectedState: Map[DagVertex, String] = singlePartitionPresentInAllVertices.keySet.map(_ -> Complete).toMap
      val state = DagState.empty.updated(StateInitializedEvent(singlePartitionPresentInAllVertices))
      assert(state.getVertexStatesByPartition.size == 1)
      assertResult(expectedState)(state.getVertexStatesFor(1L))
    }

    "should load DAG with base partitions only" in {
      val singlePartitionInBaseVertexOnly: Map[DagVertex, Set[DagPartition]] = Map(1 -> Set(1L)) ++ (2 to 7).map(_ -> Set.empty[Long]).toMap
      val expectedState: Map[DagVertex, String] = Map(1 -> Complete) ++ (2 to 7).map(_ -> Pending).toMap
      val state = DagState.empty.updated(StateInitializedEvent(singlePartitionInBaseVertexOnly))
      assertResult(expectedState)(state.getVertexStatesFor(1L))
    }

    "should ignore partitions that are not on Complete DAG path" - {
      "when the only parent has missing partition" in {
        val interruptedPartition: Map[DagVertex, Set[DagPartition]] = (1 to 7).map(_ -> Set(1L)).toMap ++ Map(4 -> Set.empty[Long])
        val expectedState: Map[DagVertex, String] = (1 to 7).map(_ -> Complete).toMap ++ Map(4 -> Pending, 7 -> Pending)
        val state = DagState.empty.updated(StateInitializedEvent(interruptedPartition))
        assertResult(expectedState)(state.getVertexStatesFor(1L))
      }
      "when one of parents has missing partition" in {
        val interruptedPartition: Map[DagVertex, Set[DagPartition]] = (1 to 7).map(_ -> Set(1L)).toMap ++ Map(2 -> Set.empty[Long])
        val expectedState: Map[DagVertex, String] = (1 to 7).map(_ -> Complete).toMap ++ Map(2 -> Pending, 6 -> Pending)
        val state = DagState.empty.updated(StateInitializedEvent(interruptedPartition))
        assertResult(expectedState)(state.getVertexStatesFor(1L))
      }
    }
  }

  "get pending to progress partitions" - {
    "from a usual state" in {
      val initialState: Map[DagVertex, String] = Map(1 -> Complete) ++ (2 to 7).map(_ -> Pending).toMap
      val state = DagState(TreeMap((1L, initialState)), Set.empty)
      val expectedResult =
        TreeSet(
          Dependency(1L, Set(1), 2),
          Dependency(1L, Set(1), 3),
          Dependency(1L, Set(1), 4),
          Dependency(1L, Set(1), 5)
        )
      assertResult(expectedResult)(state.getPendingToProgressPartitions(edges))
    }

    "when pending has multiple parents" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(6 -> Pending)
      val state = DagState(TreeMap((1L, initialState)), Set.empty)
      val expectedResult = TreeSet(Dependency(1L, Set(3,2), 6))
      assertResult(expectedResult)(state.getPendingToProgressPartitions(edges))
    }

    "when one of ancestors is not complete" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Set(3,6).map(_ -> Pending)
      val state = DagState(TreeMap((1L, initialState)), Set.empty)
      val expectedResult = TreeSet(Dependency(1L, Set(1), 3))
      assertResult(expectedResult)(state.getPendingToProgressPartitions(edges))
    }
  }

  "make pending partitions progress" in {
    val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(6 -> Pending)
    val state = DagState(TreeMap((1L, initialState)), Set.empty)
    val p2p = state.getPendingToProgressPartitions(edges)
    val expectedState: Map[DagVertex, String] = initialState + (6 -> InProgress)
    assert(p2p.size == 1)
    val actualState = state.updated(SaturationInitializedEvent(TreeSet(p2p.head)))
    assertResult(expectedState)(actualState.getVertexStatesFor(1L))
  }

  "make progressing partitions complete" in {
    val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(6 -> InProgress)
    val state = DagState(TreeMap((1L, initialState)), Set.empty)
    val expectedState: Map[DagVertex, String] = initialState + (6 -> Complete)
    val actualState = state.updated(SaturationSucceededEvent(Dependency(1L, Set(2,3), 6)))
    assertResult(expectedState)(actualState.getVertexStatesFor(1L))
  }

  "make progressing partitions fail" in {
    val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(6 -> InProgress)
    val state = DagState(TreeMap((1L, initialState)), Set.empty)
    val actualState = state.updated(SaturationFailedEvent(Dependency(1L, Set(2,3), 6)))
    assert(actualState.getVertexStatesByPartition.isEmpty)
    assertResult(Set(FailedDependency(1L, Set(2,3), 6)))(actualState.getFailedDependencies)
  }

  "make descendants of new base partition pending" in {
    val initialState = (1 to 7).map(_ -> Complete).toMap
    val state = DagState(TreeMap((1L, initialState)), Set.empty)
    val expectedState: Map[DagVertex, String] = Map(1 -> Complete) ++ (2 to 7).map(_ -> Pending)
    val actualState = state.updated(PartitionCreatedEvent(2L))
    assertResult(expectedState)(actualState.getVertexStatesFor(2L))
  }

  "invalidate partition with DAG state properly set" - {
    val initialState = (1 to 7).map(_ -> Complete).toMap
    val state = DagState(TreeMap((1L, initialState)), Set.empty)
    val expectedState: Map[DagVertex, String] = initialState ++ Set(4,7).map(_ -> Pending)
    val actualState = state.updated(PartitionVertexRemovedEvent(1L, 4))
    assertResult(expectedState)(actualState.getVertexStatesFor(1L))
  }
}