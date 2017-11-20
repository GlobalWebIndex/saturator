package gwi.s8

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FreeSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

class DagSpec extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private[this] val dag =
    Dag[Int](
      Set(
        1 -> 2, 2 -> 6,
        1 -> 3, 3 -> 6,
        1 -> 4, 4 -> 7,
        1 -> 5
      )
    )

  "root" in {
    assertResult(1)(dag.root)
  }

  "descendantsOf" in {
    assertResult((2 to 7).toSet)(dag.descendantsOf(1)())
    assertResult(Set.empty)(dag.descendantsOf(5)())

    assertResult((2 to 7).toSet)(dag.descendantsOf(1)())
    assertResult(Set.empty)(dag.descendantsOf(5)())
    assertResult(Set.empty)(dag.descendantsOf(1)(x => x < 2))
    assertResult(Set.empty)(dag.descendantsOf(1)(_ => false))
    assertResult(Set(2,4,5,7))(dag.descendantsOf(1)(x => x != 3))
  }

  "descendantsOfRoot" in
    assertResult((2 to 7).toSet)(dag.descendantsOfRoot())

  "neighborDescendantsOf" in {
    assertResult((2 to 5).toSet)(dag.neighborDescendantsOf(1))
    assertResult(Set.empty)(dag.neighborDescendantsOf(5))
  }

  "neighborDescendantsOfRoot" in
    assertResult((2 to 5).toSet)(dag.neighborDescendantsOfRoot)

  "neighborAncestorsOf" in {
    assertResult((2 to 3).toSet)(dag.neighborAncestorsOf(6))
    assertResult(Set(4))(dag.neighborAncestorsOf(7))
    assertResult(Set.empty)(dag.neighborAncestorsOf(1))
  }

  "ancestorsOf" in {
    assertResult(Set(1,4))(dag.ancestorsOf(7))
    assertResult(Set(1,2,3))(dag.ancestorsOf(6))
    assertResult(Set.empty)(dag.ancestorsOf(1))
  }

  "families" in {
    assertResult((2 to 5).map(Set(1) -> _).toSet + (Set(2,3) -> 6) + (Set(4) -> 7))(dag.families)
  }

}