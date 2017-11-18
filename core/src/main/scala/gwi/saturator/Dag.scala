package gwi.saturator

protected[saturator] case class Dag[V](edges: Set[(V,V)]) {

  def root: V = {
    val ancestorLessVertices = edges.flatMap(v => Set(v._1, v._2)) -- edges.map(_._2)
    require(ancestorLessVertices.size == 1, "DAG must be rooted !!!")
    ancestorLessVertices.head
  }

  def allVertices: Set[V] = edges.flatMap(edge => Set(edge._1, edge._2))

  def descendantsOf(v: V, actualEdges: Set[(V,V)] = edges)(condition: V => Boolean = (_: V) => true): Set[V] = {
    val (outgoingEdges, remainingEdges) = actualEdges.partition(_._1 == v)
    val neighborDescendants = outgoingEdges.map(_._2).filter(t => condition(t) && ancestorsOf(t, actualEdges).forall(condition))
    neighborDescendants ++ neighborDescendants.flatMap(descendantsOf(_, remainingEdges)(condition))
  }

  def descendantsOfRoot(condition: V => Boolean = (_: V) => true): Set[V] =
    descendantsOf(root)(condition)

  def neighborDescendantsOf(v: V): Set[V] =
    edges.collect { case (source,target) if source == v => target }

  def neighborDescendantsOfRoot: Set[V] =
    neighborDescendantsOf(root)

  def neighborAncestorsOf(v: V): Set[V] =
    edges.collect { case (source,target) if target == v => source }

  def ancestorsOf(v: V, actualEdges: Set[(V,V)] = edges): Set[V] = {
    val (incoming, remainingEdges) = actualEdges.partition(_._2 == v)
    val neighborAncestors = incoming.map(_._1)
    neighborAncestors ++ neighborAncestors.flatMap(ancestorsOf(_, remainingEdges))
  }

  def families: Set[(Set[V],V)] =
    edges.groupBy(_._2).mapValues(_.map(_._1)).toSet[(V,Set[V])].map { case (target, sources) => sources -> target }

}
