import java.io.File

import sbt._

import scala.collection.immutable.TreeSet

object DockerUtils {

  /**
    * @note this method exists because Dockerfile position of app-deps.jar should be always before app.jar because deps don't change usually
    */
  def dockerCopySorted[T](sourceDirectory: String, targetDirectory: String)(copy: (String, String) => T): Unit = {
    def recursively(dir: File): TreeSet[File] = {
      val list = TreeSet(sbt.IO.listFiles(dir):_*)
      list.filter(_.isFile) ++ list.filter(_.isDirectory).flatMap(recursively)
    }
    recursively(new File(sourceDirectory)).toList
      .map(_.absolutePath)
      .map( path => path.substring(sourceDirectory.length) )
      .foreach( partialPath => copy(sourceDirectory + partialPath, targetDirectory + partialPath) )
  }

}