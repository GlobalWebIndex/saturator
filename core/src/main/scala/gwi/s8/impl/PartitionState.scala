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
    private[this] def collectSaturationErrors(sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex): List[String] = {
      val neighborDescendants = dag.neighborDescendantsOf(targetVertex)
      List(
        Option(sourceVertices.forall(underlying(_) == Complete)).filterNot(identity).map(_ => "Illegal saturation from inComplete vertex"),
        Option(underlying(targetVertex) == InProgress).filterNot(identity).map(_ => "Illegal saturation of not progressing vertex"),
        Option(underlying.filterKeys(neighborDescendants.contains).forall(_._2 == Pending)).filterNot(identity).map(_ => "Dep saturated with target descendants not pending")
      ).flatten
    }

    private[impl] def fix: Either[String, PartitionState] = {
      val rootVertex = dag.root
      val rootDescendants = dag.descendantsOf(rootVertex)()
      if (!rootDescendants.map(underlying).contains(InProgress)) {
        val newState = underlying ++ rootDescendants.filter(underlying(_) == Failed).map(_ -> Pending)
        logger.info(s"\n$printable\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.printable}")
        Right(newState)
      } else {
        Left(s"Fixing progressing partition is not allowed !!!\n $printable")
      }
    }

    private[impl] def redo(vertex: DagVertex): Either[String, PartitionState] = {
      val branch = dag.meAndMyDescendantsUnlessRoot(vertex)
      val branchStates = branch.map(underlying)
      val ancestorStates = dag.ancestorsOf(vertex).map(underlying)
      if (branchStates.contains(InProgress)) {
        Left(s"Redoing dag branch of $vertex in progressing partition is not allowed !!!\n $printable")
      } else if (!ancestorStates.forall(_ == Complete)){
        Left(s"Dag branch cannot be redone in vertex $vertex from incomplete ancestors !!!\n $printable")
      } else {
        val newState = underlying ++ branch.map(_ -> Pending)
        logger.info(s"\n$printable\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.printable}")
        Right(underlying ++ branch.map(_ -> Pending))
      }
    }

    // TODO look at errors and decide what to do with DAG
    private[impl] def fail(sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex): Either[String, PartitionState] = {
      val errors = collectSaturationErrors(sourceVertices, targetVertex)
      if (errors.nonEmpty) {
        Left(s"${errors.mkString("\n","\n","\n")}$printable")
      } else {
        val newState = underlying.updated(targetVertex, Failed)
        logger.info(s"\n$printable\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.printable}")
        Right(newState)
      }
    }

    // TODO look at errors and decide what to do with DAG
    private[impl] def succeed(sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex): Either[String, PartitionState] = {
      val errors = collectSaturationErrors(sourceVertices, targetVertex)
      if (errors.nonEmpty) {
        Left(s"${errors.mkString("\n","\n","\n")}$printable")
      } else {
        val newState = underlying.updated(targetVertex, Complete)
        logger.info(s"\n$printable\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.printable}")
        Right(newState)
      }
    }

    // TODO look at errors and decide what to do with DAG
    private[impl] def progress(sourceVertices: SortedSet[DagVertex], targetVertex: DagVertex): Either[String, PartitionState] =
      if (underlying(targetVertex) != Pending) {
        Left(s"Progressing vertex $targetVertex is not Pending !!! \n$printable")
      } else if (sourceVertices.forall(underlying(_) != Complete)) {
        Left(s"Vertex $targetVertex cannot progress from incomplete vertices !!! \n$printable")
      } else {
        val newState = underlying.updated(targetVertex, InProgress)
        logger.info(s"\n$printable\nvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n${newState.printable}")
        Right(newState)
      }

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
