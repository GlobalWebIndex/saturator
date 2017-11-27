package gwi.s8.impl

import scala.collection.breakOut
import scala.collection.immutable.{SortedSet, TreeSet}
import scala.math.Ordering

private[impl] case class Dag[V](edges: Set[(V,V)])(implicit vo: Ordering[V]) {

  private[impl] def root: V = {
    val ancestorLessVertices = edges.flatMap(v => Set(v._1, v._2)) -- edges.map(_._2)
    require(ancestorLessVertices.size == 1, "DAG must be rooted !!!")
    ancestorLessVertices.head
  }

  private[impl] def allVertices: SortedSet[V] = edges.flatMap(edge => TreeSet(edge._1, edge._2))(breakOut)

  private[impl] def descendantsOf(v: V, actualEdges: Set[(V,V)] = edges)(condition: V => Boolean = (_: V) => true): SortedSet[V] = {
    val (outgoingEdges, remainingEdges) = actualEdges.partition(_._1 == v)
    val neighborDescendants: TreeSet[V] =
      outgoingEdges.collect {
        case (_,child) if condition(child) && ancestorsOf(child, actualEdges).forall(condition) => child
      }(breakOut)
    neighborDescendants ++ neighborDescendants.flatMap(descendantsOf(_, remainingEdges)(condition))
  }

  private[impl] def descendantsOfRoot(condition: V => Boolean = (_: V) => true): SortedSet[V] =
    descendantsOf(root)(condition)

  private[impl] def neighborDescendantsOf(v: V): SortedSet[V] =
    edges.collect { case (source,target) if source == v => target }(breakOut)

  private[impl] def neighborDescendantsOfRoot: SortedSet[V] =
    neighborDescendantsOf(root)

  private[impl] def neighborAncestorsOf(v: V): SortedSet[V] =
    edges.collect { case (source,target) if target == v => source }(breakOut)

  private[impl] def ancestorsOf(v: V, actualEdges: Set[(V,V)] = edges): SortedSet[V] = {
    val (incoming, remainingEdges) = actualEdges.partition(_._2 == v)
    val neighborAncestors: TreeSet[V] = incoming.map(_._1)(breakOut)
    neighborAncestors ++ neighborAncestors.flatMap(ancestorsOf(_, remainingEdges))
  }

  private[impl] def families: Set[(Set[V],V)] =
    edges.groupBy(_._2).mapValues(_.map(_._1)).toSet[(V,Set[V])].map { case (target, sources) => sources -> target }

  private[impl] def meAndMyDescendantsUnlessRoot(v: V): SortedSet[V] = descendantsOf(v)() ++ Option(v).filter(_ != root)
  private[impl] def meAndMyAncestors(v: V): SortedSet[V] = ancestorsOf(v) + v

}
