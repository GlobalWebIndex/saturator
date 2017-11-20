package gwi.s8

import collection.immutable.{SortedSet, TreeSet}
import math.Ordering
import collection.breakOut

protected[s8] case class Dag[V](edges: Set[(V,V)])(implicit vo: Ordering[V]) {

  def root: V = {
    val ancestorLessVertices = edges.flatMap(v => Set(v._1, v._2)) -- edges.map(_._2)
    require(ancestorLessVertices.size == 1, "DAG must be rooted !!!")
    ancestorLessVertices.head
  }

  def allVertices: SortedSet[V] = edges.flatMap(edge => TreeSet(edge._1, edge._2))(breakOut)

  def descendantsOf(v: V, actualEdges: Set[(V,V)] = edges)(condition: V => Boolean = (_: V) => true): SortedSet[V] = {
    val (outgoingEdges, remainingEdges) = actualEdges.partition(_._1 == v)
    val neighborDescendants: TreeSet[V] =
      outgoingEdges.collect {
        case (_,child) if condition(child) && ancestorsOf(child, actualEdges).forall(condition) => child
      }(breakOut)
    neighborDescendants ++ neighborDescendants.flatMap(descendantsOf(_, remainingEdges)(condition))
  }

  def descendantsOfRoot(condition: V => Boolean = (_: V) => true): SortedSet[V] =
    descendantsOf(root)(condition)

  def neighborDescendantsOf(v: V): SortedSet[V] =
    edges.collect { case (source,target) if source == v => target }(breakOut)

  def neighborDescendantsOfRoot: SortedSet[V] =
    neighborDescendantsOf(root)

  def neighborAncestorsOf(v: V): SortedSet[V] =
    edges.collect { case (source,target) if target == v => source }(breakOut)

  def ancestorsOf(v: V, actualEdges: Set[(V,V)] = edges): SortedSet[V] = {
    val (incoming, remainingEdges) = actualEdges.partition(_._2 == v)
    val neighborAncestors: TreeSet[V] = incoming.map(_._1)(breakOut)
    neighborAncestors ++ neighborAncestors.flatMap(ancestorsOf(_, remainingEdges))
  }

  def families: Set[(Set[V],V)] =
    edges.groupBy(_._2).mapValues(_.map(_._1)).toSet[(V,Set[V])].map { case (target, sources) => sources -> target }

}
