package gwi.saturator

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FreeSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

class DagSpec extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  import Dag._

  val edges =
    Set(
      1 -> 2,
      1 -> 3,
      1 -> 4,
      1 -> 5,
      2 -> 6,
      3 -> 6,
      4 -> 7
    )

  "root" in {
    assertResult(1)(root(edges))
  }

  "descendantsOf" in {
    assertResult((2 to 7).toSet)(descendantsOf(1, edges))
    assertResult(Set.empty)(descendantsOf(5, edges))
  }

  "descendantsOfRoot" in
    assertResult((2 to 7).toSet)(descendantsOfRoot(edges))

  "neighborDescendantsOf" in {
    assertResult((2 to 5).toSet)(neighborDescendantsOf(1, edges))
    assertResult(Set.empty)(neighborDescendantsOf(5, edges))
  }

  "neighborDescendantsOfRoot" in
    assertResult((2 to 5).toSet)(neighborDescendantsOfRoot(edges))

  "neighborAncestorsOf" in {
    assertResult((2 to 3).toSet)(neighborAncestorsOf(6, edges))
    assertResult(Set(4))(neighborAncestorsOf(7, edges))
    assertResult(Set.empty)(neighborAncestorsOf(1, edges))
  }

  "ancestorsOf" in {
    assertResult(Set(1,4))(ancestorsOf(7, edges))
    assertResult(Set(1,2,3))(ancestorsOf(6, edges))
    assertResult(Set.empty)(ancestorsOf(1, edges))
  }

  "families" in {
    assertResult((2 to 5).map(Set(1) -> _).toSet + (Set(2,3) -> 6) + (Set(4) -> 7))(families(edges))
  }

}