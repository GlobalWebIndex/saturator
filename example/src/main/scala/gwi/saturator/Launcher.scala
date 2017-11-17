package gwi.saturator

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import gwi.saturator.DagFSM.{Issued, PartitionChangesSchedule}
import gwi.saturator.DagMock._
import gwi.saturator.SaturatorCmd._
import org.backuity.clist._
import org.backuity.clist.util.Read

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._

object Launcher extends CliMain[Unit] {

  private implicit def listRead: Read[List[Int]] = Read.reads("list") { str =>
    str.split(",").filter(_.nonEmpty).map(_.toInt).toList
  }

  private implicit def edgesRead: Read[List[(DagVertex, DagVertex)]] =
    Read.reads("edges")(_.split(",").filter(_.nonEmpty).map(_.split("-")).map(arr => VertexMock(arr(0)) -> VertexMock(arr(1))).toList)

  var edges                   = arg[List[(DagVertex,DagVertex)]]()
  var existingHeadPartitions  = arg[List[Int]](name = "existing-head-partitions")
  var newPartitionInterval    = arg[Int](name = "new-partition-interval")
  var persistencePlugin       = arg[String](name = "persistence-plugin")

  override def run: Unit = {
    val system = ActorSystem("example", ConfigFactory.load(persistencePlugin).withFallback(ConfigFactory.parseResources("reference.conf")).resolve())

    val vertices = edges.flatMap(t => Set(t._1, t._2)).toSet
    val headVertex = edges.head._1
    val tailVertices = vertices - headVertex
    def partitionsByVertex: List[(DagVertex, List[DagPartition])] =
      List(headVertex -> existingHeadPartitions.map(p => PartitionMock(p.toString))) ++ tailVertices.map(v => v -> List.empty[DagPartition])

    system.actorOf(Props(classOf[Example], edges.toSet, partitionsByVertex _, newPartitionInterval.seconds, existingHeadPartitions.last))
  }
}

class Example(edges: Set[(DagVertex,DagVertex)], init: => List[(DagVertex, List[DagPartition])], interval: FiniteDuration, lastPartition: Int) extends Actor with ActorLogging {
  private[this] var partitionCounter = lastPartition + 1
  private[this] implicit val e = edges
  private[this] val dagFSM = DagFSM(init, self, Some(PartitionChangesSchedule(interval, interval)), "example-dag-fsm")

  def receive: Receive = {
    case Issued(GetPartitionChanges(_),_,_,_) =>
      dagFSM ! PartitionInserts(TreeSet(PartitionMock(partitionCounter.toString)))
      log.info(s"New partition created ${partitionCounter.toString} ...")
      partitionCounter+=1
    case Issued(Saturate(deps), _, _, _) =>
      deps.foreach { dep =>
        log.info("Saturating {}", dep)
        dagFSM ! SaturationResponse(dep,true)
      }
  }
}