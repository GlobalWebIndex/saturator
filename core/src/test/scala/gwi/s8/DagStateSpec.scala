package gwi.s8

import gwi.s8.DagFSMState._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FreeSpec, Matchers}

import collection.immutable.{TreeMap, TreeSet}

class DagStateSpec extends FreeSpec with DagTestSupport with ScalaFutures with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  import PartitionState._
  private[this] implicit val edges: Set[(DagVertex, DagVertex)] =
    Set(
      1 -> 2, 2 -> 6,
      1 -> 3, 3 -> 6,
      1 -> 4, 4 -> 7,
      1 -> 5
    )

  "loading DAG" - {
    "should ignore alien partitions" in {
      val partitionsByVertex = Map[Int, Set[Long]](
        1 -> Set(1L, 2L),
        2 -> Set(1L, 2L),
        3 -> Set(1L, 2L, 3L)
      )
      val stateByVertex: PartitionState = (1 to 3).map(_ -> Complete).toMap ++ (4 to 7).map(_ -> Pending).toMap
      val expectedState: PartitionedDagState = (1 to 2).map( _ -> stateByVertex).toMap
      assertResult(expectedState)(DagFSMState.empty.updated(StateInitializedEvent(partitionsByVertex)).partitionedDagState)
    }

    "should load complete DAG" in {
      val singlePartitionPresentInAllVertices: Map[DagVertex, Set[DagPartition]] = (1 to 7).map(_ -> Set(1L)).toMap
      val expectedState: Map[DagVertex, String] = singlePartitionPresentInAllVertices.keySet.map(_ -> Complete).toMap
      val state = DagFSMState.empty.updated(StateInitializedEvent(singlePartitionPresentInAllVertices))
      assert(state.partitionedDagState.size == 1)
      assertResult(expectedState)(state.partitionedDagState(1L))
      assert(state.isSaturated)
    }

    "should load DAG with base partitions only" in {
      val singlePartitionInBaseVertexOnly: Map[DagVertex, Set[DagPartition]] = Map(1 -> Set(1L)) ++ (2 to 7).map(_ -> Set.empty[Long]).toMap
      val expectedState: PartitionState = Map(1 -> Complete) ++ (2 to 7).map(_ -> Pending).toMap
      val state = DagFSMState.empty.updated(StateInitializedEvent(singlePartitionInBaseVertexOnly))
      assertResult(expectedState)(state.partitionedDagState(1L))
      assert(!state.isSaturated)
    }

    "should ignore partitions that are not on Complete DAG path" - {
      "when the only parent has missing partition" in {
        val interruptedPartition: Map[DagVertex, Set[DagPartition]] = (1 to 7).map(_ -> Set(1L)).toMap ++ Map(4 -> Set.empty[Long])
        val expectedState: PartitionState = (1 to 7).map(_ -> Complete).toMap ++ Map(4 -> Pending, 7 -> Pending)
        val state = DagFSMState.empty.updated(StateInitializedEvent(interruptedPartition))
        assertResult(expectedState)(state.partitionedDagState(1L))
        assert(!state.isSaturated)
      }
      "when one of parents has missing partition" in {
        val interruptedPartition: Map[DagVertex, Set[DagPartition]] = (1 to 7).map(_ -> Set(1L)).toMap ++ Map(2 -> Set.empty[Long])
        val expectedState: PartitionState = (1 to 7).map(_ -> Complete).toMap ++ Map(2 -> Pending, 6 -> Pending)
        val state = DagFSMState.empty.updated(StateInitializedEvent(interruptedPartition))
        assertResult(expectedState)(state.partitionedDagState(1L))
        assert(!state.isSaturated)
      }
    }
  }

  "get pending to progress partitions" - {
    "from a usual state" in {
      val initialState: PartitionState = Map(1 -> Complete) ++ (2 to 7).map(_ -> Pending).toMap
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val expectedResult =
        TreeSet(
          Dependency(1L, TreeSet(1), 2),
          Dependency(1L, TreeSet(1), 3),
          Dependency(1L, TreeSet(1), 4),
          Dependency(1L, TreeSet(1), 5)
        )
      assertResult(expectedResult)(state.getPendingDeps)
      assert(!state.isSaturated)
    }

    "when pending has multiple parents" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(6 -> Pending)
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val expectedResult = TreeSet(Dependency(1L, TreeSet(3,2), 6))
      assertResult(expectedResult)(state.getPendingDeps)
      assert(!state.isSaturated)
    }

    "when one of ancestors is not complete" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ TreeSet(3,6).map(_ -> Pending)
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val expectedResult = TreeSet(Dependency(1L, TreeSet(1), 3))
      assertResult(expectedResult)(state.getPendingDeps)
      assert(!state.isSaturated)
    }

    "when one of ancestors has failed" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(4 -> Failed, 7 -> Pending)
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val expectedResult = TreeSet.empty[Dependency]
      assertResult(expectedResult)(state.getPendingDeps)
      assert(state.isSaturated)
    }

    "when one of neighbor ancestors has failed" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(3 -> Failed, 6 -> Pending)
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val expectedResult = TreeSet.empty[Dependency]
      assertResult(expectedResult)(state.getPendingDeps)
      assert(state.isSaturated)
    }

    "make pending partitions progress" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(6 -> Pending)
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val p2p = state.getPendingDeps
      assert(p2p.size == 1)
      val expectedState: PartitionState = initialState + (6 -> InProgress)
      val actualState = state.updated(SaturationInitializedEvent)
      assertResult(expectedState)(actualState.partitionedDagState(1L))
      assert(!actualState.isSaturated)
    }
  }


  "make progressing partitions complete" in {
    val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(6 -> InProgress)
    val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
    val expectedState: PartitionState = initialState + (6 -> Complete)
    val actualState = state.updated(SaturationSucceededEvent(Dependency(1L, TreeSet(2,3), 6)))
    assertResult(expectedState)(actualState.partitionedDagState(1L))
    assert(actualState.isSaturated)
  }

  "make descendants of new base partition pending" in {
    val initialState = (1 to 7).map(_ -> Complete).toMap
    val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
    val expectedState: PartitionState = Map(1 -> Complete) ++ (2 to 7).map(_ -> Pending)
    val actualState = state.updated(PartitionInsertsEvent(TreeSet(2L)))
    assertResult(expectedState)(actualState.partitionedDagState(2L))
    assert(!actualState.isSaturated)
  }

  "redo dag branch" in {
    val initialState = (1 to 7).map(_ -> Complete).toMap
    val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
    val expectedState: PartitionState = initialState ++ TreeSet(4,7).map(_ -> Pending)
    val actualState = state.updated(DagBranchRedoEvent(1L, 4))
    assertResult(expectedState)(actualState.partitionedDagState(1L))
    assert(!actualState.isSaturated)
  }

  "fix partition" in {
    val initialState = (1 to 7).map(_ -> Pending).toMap ++ TreeSet(1,3,4).map(_ -> Complete) ++ Map(2 -> Failed)
    val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
    val expectedState: PartitionState = initialState ++ Map(2 -> Pending)
    val actualState = state.updated(PartitionFixEvent(1L))
    assertResult(expectedState)(actualState.partitionedDagState(1L))
    assert(!actualState.isSaturated)
  }

  "redo whole partition" in {
    val initialState = (1 to 7).map(_ -> Pending).toMap ++ TreeSet(1,3,4).map(_ -> Complete) ++ Map(2 -> Failed)
    val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
    val expectedState: PartitionState = (1 to 7).map(_ -> Pending).toMap ++ Map(1 -> Complete)
    val actualState = state.updated(DagBranchRedoEvent(1L, 1))
    assertResult(expectedState)(actualState.partitionedDagState(1L))
    assert(!actualState.isSaturated)
  }

  "error handling" - {
    "failing leaf dependency" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(7 -> InProgress)
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val expectedState: PartitionState = initialState ++ Map(7 -> Failed)
      val actualState = state.updated(SaturationFailedEvent(Dependency(1L, TreeSet(4), 7)))
      assertResult(expectedState)(actualState.partitionedDagState(1L))
      assert(actualState.isSaturated)
    }

    "failing one of multiple source vertices dependency" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(2 -> InProgress, 6 -> Pending)
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val expectedState: PartitionState = initialState ++ Map(2 -> Failed)
      val actualState = state.updated(SaturationFailedEvent(Dependency(1L, TreeSet(1), 2)))
      assertResult(expectedState)(actualState.partitionedDagState(1L))
      assert(actualState.isSaturated)
    }

    "failing dependency on multiple vertices" in {
      val initialState = (1 to 7).map(_ -> Complete).toMap ++ Map(6 -> InProgress)
      val state = DagFSMState(TreeMap((1L, initialState)), Set.empty)
      val expectedState: PartitionState = initialState ++ Map(6 -> Failed)
      val actualState = state.updated(SaturationFailedEvent(Dependency(1L, TreeSet(2,3), 6)))
      assertResult(expectedState)(actualState.partitionedDagState(1L))
      assert(actualState.isSaturated)
    }
  }

}