package gwi.s8.impl

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.typesafe.scalalogging.StrictLogging
import gwi.s8.{DagPartition, DagVertex}

import scala.collection.breakOut
import scala.collection.immutable.{SortedSet, TreeMap, TreeSet}
import scala.math.Ordering

private[impl] object PartitionState extends StrictLogging {

  private[impl] val Pending     = "Pending"
  private[impl] val Complete    = "Complete"
  private[impl] val InProgress  = "InProgress"
  private[impl] val Failed      = "Failed"

  private[impl] def apply()(implicit dag: Dag[DagVertex], vo: Ordering[DagVertex]) =
    TreeMap(dag.allVertices.map(v => v -> (if (v == dag.root) Complete else Pending)).toSeq:_*)

  private[impl] def buildGraphForPartition(p: DagPartition, vertex: DagVertex, partitionsByVertex: Map[DagVertex, Set[DagPartition]])(implicit dag: Dag[DagVertex], vo: Ordering[DagVertex]): PartitionState = {
    def isComplete = dag.meAndMyAncestors(vertex).forall(partitionsByVertex.get(_).exists(_.contains(p)))
    val state = if (vertex == dag.root || isComplete) Complete else Pending
    val partitionState = dag.neighborDescendantsOf(vertex).flatMap(buildGraphForPartition(p, _, partitionsByVertex)).toMap + (vertex -> state)
    TreeMap(partitionState.toSeq:_*)
  }

  private[impl] implicit class PartitionStatePimp(underlying: PartitionState)(implicit dag: Dag[DagVertex], vo: Ordering[DagVertex]) {
    private[this] def collectErrors(result: => PartitionState)(condError: (Boolean, String)*): Either[String, PartitionState] = {
      val errors = condError.flatMap { case (cond, error) => Option(cond).filterNot(identity).map(_ => error) }
      if (errors.isEmpty)
        Right(result)
      else
        Left(errors.mkString("\n","\n","\n"))
    }

    private[impl] def fix: Either[String, PartitionState] = {
      val rootVertex = dag.root
      val rootDescendants = dag.descendantsOf(rootVertex)()
      collectErrors(underlying ++ rootDescendants.filter(underlying(_) == Failed).map(_ -> Pending)) {
        !rootDescendants.map(underlying).contains(InProgress) -> s"Fixing progressing partition is not allowed !!!"
      }
    }

    private[impl] def redo(vertex: DagVertex): Either[String, PartitionState] = {
      val branch = dag.meAndMyDescendantsUnlessRoot(vertex)
      val branchStates = branch.map(underlying)
      val ancestorStates = dag.ancestorsOf(vertex).map(underlying)
      collectErrors(underlying ++ branch.map(_ -> Pending)) (
        !branchStates.contains(InProgress) -> s"Redoing dag branch of $vertex in progressing partition is not allowed !!!",
        ancestorStates.forall(_ == Complete) -> s"Dag branch cannot be redone in vertex $vertex from incomplete ancestors !!!"
      )
    }

    private[impl] def fail(sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex): Either[String, PartitionState] =
      collectErrors(underlying.updated(targetVertex, Failed))(
        sourceVertices.forall(underlying(_) == Complete) -> "Illegal saturation from inComplete vertex",
        (underlying(targetVertex) == InProgress) -> "Illegal saturation of not progressing vertex",
        underlying.filterKeys(dag.neighborDescendantsOf(targetVertex).contains).forall(_._2 == Pending) -> "Dep saturated with target descendants not pending"
      )

    private[impl] def succeed(sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex): Either[String, PartitionState] =
      collectErrors(underlying.updated(targetVertex, Complete)) (
        sourceVertices.forall(underlying(_) == Complete) -> "Illegal saturation from inComplete vertex",
        (underlying(targetVertex) == InProgress) -> "Illegal saturation of not progressing vertex",
        underlying.filterKeys(dag.neighborDescendantsOf(targetVertex).contains).forall(_._2 == Pending) -> "Dep saturated with target descendants not pending"
      )

    private[impl] def progress(sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex): Either[String, PartitionState] =
      collectErrors(underlying.updated(targetVertex, InProgress)) (
        (underlying(targetVertex) == Pending) -> s"Progressing vertex $targetVertex is not Pending !!!",
        sourceVertices.forall(underlying(_) == Complete) -> s"Vertex $targetVertex cannot progress from incomplete vertices !!!"
      )

    private[impl] def isSaturated: Boolean =
      underlying(dag.root) == Complete && dag.descendantsOfRoot(v => underlying(v) != Failed).forall(v => underlying(v) == Complete)

    private[impl] def getPendingVertices: SortedSet[DagVertex] =
      TreeSet(underlying.collect { case (v, ps) if ps == Pending && dag.ancestorsOf(v).forall(underlying(_) == Complete) => v }.toSeq:_*)

    private[impl] def getProgressingVertices: SortedSet[DagVertex] =
      TreeSet(underlying.collect { case (v, ps) if ps == InProgress && dag.ancestorsOf(v).forall(underlying(_) == Complete) => v }.toSeq:_*)

    private[impl] def printable: String = {
      val stateByVertex = underlying.map { case (v, state) => v.vid -> state }
      val edgesWithState: List[(String, String)] = dag.edges.map(t => t._1.vid -> t._2.vid).map { case (f, t) => s"$f\n${stateByVertex(f)}" -> s"$t\n${stateByVertex(t)}" }(breakOut)
      val verticesWithState: Set[String] = stateByVertex.map { case (v, state) => s"$v\n$state" }(breakOut)
      GraphLayout.renderGraph(Graph(verticesWithState, edgesWithState))
    }
  }

}
