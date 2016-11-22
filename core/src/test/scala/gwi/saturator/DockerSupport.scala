package gwi.saturator

import scala.sys.process.Process
import scala.util.Try

trait DockerSupport {

  private def docker(cmd: String) = Array("/bin/sh", "-c", s"docker $cmd")

  protected def startContainer(image: String, name: String, port: Int)(prepare: => Unit): Unit = {
    require(Process(docker(s"run --name $name -p $port:$port -d $image")).run().exitValue == 0)
    Thread.sleep(500)
    prepare
  }

  protected def stopContainer(name: String)(cleanup: => Unit): Unit = {
    Try(cleanup)
    Process(docker(s"stop $name")).run()
    Process(docker(s"rm -fv $name")).run()
  }
}