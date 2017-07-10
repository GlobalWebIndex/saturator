package gwi.saturator

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.typesafe.config.ConfigValueFactory
import gwi.saturator.DagFSM.{CreatePartition, Saturate, SaturationResponse}
import org.backuity.clist._
import org.backuity.clist.util.Read
import DagMock._
import scala.concurrent.duration._

object Launcher extends CliMain[Unit] {

  implicit def listRead: Read[List[Int]] = Read.reads("list") { str =>
    str.split(",").filter(_.nonEmpty).map(_.toInt).toList
  }

  implicit def edgesRead: Read[List[(DagVertex, DagVertex)]] =
    Read.reads("edges")(_.split(",").filter(_.nonEmpty).map(_.split("-")).map(arr => VertexMock(arr(0)) -> VertexMock(arr(1))).toList)

  var edges = arg[List[(DagVertex,DagVertex)]]()
  var existingHeadPartitions = arg[List[Int]](required = true, name = "existing-head-partitions")
  var newPartitionInterval = arg[Int](name = "new-partition-interval")

  override def run: Unit = {
    val system = ActorSystem("example", DagFSMSpec.config.withValue("redis.host", ConfigValueFactory.fromAnyRef("redis")))

    val vertices = edges.flatMap(t => Set(t._1, t._2)).toSet
    val headVertex = edges.head._1
    val tailVertices = vertices - headVertex
    def partitionsByVertex: List[(DagVertex, List[DagPartition])] =
      List(headVertex -> existingHeadPartitions.map(p => PartitionMock(p.toString))) ++ tailVertices.map(v => v -> List.empty[DagPartition])

    system.actorOf(Props(classOf[Example], edges.toSet, partitionsByVertex _, newPartitionInterval.seconds, existingHeadPartitions.last))
  }
}

class Example(edges: Set[(DagVertex,DagVertex)], init: => List[(DagVertex, List[DagPartition])], interval: FiniteDuration, lastPartition: Int) extends Actor with ActorLogging {
  import context.dispatcher
  var partitionCounter = lastPartition + 1
  implicit val e = edges
  val dagFSM = DagFSM(init, self, "example-dag-fsm")

  val c = context.system.scheduler.schedule(interval, interval) {
    dagFSM ! CreatePartition(PartitionMock(partitionCounter.toString))
    partitionCounter+=1
  }

  override def postStop(): Unit = c.cancel()

  def receive: Receive = {
    case Saturate(deps) =>
      deps.foreach { dep =>
        log.info("Saturating {}", dep)
        dagFSM ! SaturationResponse(dep,true)
      }
  }
}