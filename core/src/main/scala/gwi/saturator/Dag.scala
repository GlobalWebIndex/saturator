package gwi.saturator

object Dag {
  def root[V](edges: Set[(V,V)]): V = {
    val ancestorLessVertices = edges.flatMap(v => Set(v._1, v._2)) -- edges.map(_._2)
    require(ancestorLessVertices.size == 1, "DAG must be rooted !!!")
    ancestorLessVertices.head
  }

  def allVertices[V](edges: Set[(V,V)]): Set[V] = edges.flatMap(edge => Set(edge._1, edge._2))

  def descendantsOf[V](v: V, edges: Set[(V,V)]): Set[V] = {
    val (outgoingEdges, remainingEdges) = edges.partition(_._1 == v)
    val neighborDescendants = outgoingEdges.map(_._2)
    neighborDescendants ++ neighborDescendants.flatMap(descendantsOf(_, remainingEdges))
  }

  def descendantsOfRoot[V](edges: Set[(V,V)]): Set[V] =
    descendantsOf(root(edges), edges)

  def neighborDescendantsOf[V](v: V, edges: Set[(V,V)]): Set[V] =
    edges.collect { case (source,target) if source == v => target }

  def neighborDescendantsOfRoot[V](edges: Set[(V,V)]): Set[V] =
    neighborDescendantsOf(root(edges), edges)

  def neighborAncestorsOf[V](v: V, edges: Set[(V,V)]): Set[V] =
    edges.collect { case (source,target) if target == v => source }

  def ancestorsOf[V](v: V, edges: Set[(V,V)]): Set[V] = {
    val (incoming, remainingEdges) = edges.partition(_._2 == v)
    val neighborAncestors = incoming.map(_._1)
    neighborAncestors ++ neighborAncestors.flatMap(ancestorsOf(_, remainingEdges))
  }

  def families[V](edges: Set[(V,V)]): Set[(Set[V],V)] =
    edges.groupBy(_._2).mapValues(_.map(_._1)).toSet[(V,Set[V])].map { case (target, sources) => sources -> target }

}
