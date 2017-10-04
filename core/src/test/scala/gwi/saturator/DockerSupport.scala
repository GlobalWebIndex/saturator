package gwi.saturator

import scala.sys.process.Process
import scala.util.Try

trait DockerSupport {

  case class PPorts(host: String, guest: String)
  object PPorts {
    def from(host: Int, guest: Int) = new PPorts(host.toString, guest.toString)
  }

  private def docker(cmd: String) = Array("/bin/sh", "-c", s"docker $cmd")

  private def portsToString(ports: Seq[PPorts]) =
    if (ports.isEmpty) "" else ports.map { case PPorts(host, guest) => s"$host:$guest" }.mkString("-p ", " -p ", "")

  protected def startContainer(image: String, name: String, ports: Seq[PPorts], cmd: Option[String])(prepare: => Unit): Unit = {
    require(Process(docker(s"run --name $name ${portsToString(ports)} -d $image ${cmd.getOrElse("")}")).run(true).exitValue == 0)
    Thread.sleep(500)
    prepare
  }

  protected def stopContainer(name: String)(cleanup: => Unit): Unit = {
    Try(cleanup)
    Process(docker(s"stop $name")).run()
    Process(docker(s"rm -fv $name")).run()
  }
}