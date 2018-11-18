import sbt.Keys._
import sbt._
import net.globalwebindex.sbt.docker.SmallerDockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.SbtNativePackager.autoImport._

object Deploy {

  private[this] def isFrequentlyChangingFile(file: sbt.File): Boolean = {
    val fileName = file.name
    if (fileName.startsWith("net.globalwebindex")) true
    else if (fileName.endsWith(".jar")) false
    else true
  }

  def settings(repository: String, appName: String, mainClassFqn: String): Seq[Def.Setting[_]] = {
    Seq(
      dockerUpdateLatest := false,
      dockerRepository := Some(repository),
      packageName in Docker := appName,
      dockerBaseImage in Docker := "anapsix/alpine-java:8u192b12_jdk_unlimited",
      mainClass in Compile := Some(mainClassFqn),
      defaultLinuxInstallLocation in Docker := s"/opt/$appName",
      dockerUpdateLatest in Docker := false
    ) ++ smallerDockerSettings(isFrequentlyChangingFile)
  }


  def publishSettings(organization: String, projectName: String, resolverOpt: sbt.Resolver) = Seq(
    publishTo := Some(resolverOpt),
    publishMavenStyle := true,
    publishArtifact := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    pomExtra :=
      <url>https://github.com/{organization}/{projectName}</url>
        <licenses>
          <license>
            <name>The MIT License (MIT)</name>
            <url>http://opensource.org/licenses/MIT</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:{organization}/{projectName}.git</url>
          <connection>scm:git:git@github.com:{organization}/{projectName}.git</connection>
        </scm>
        <developers>
          <developer>
            <id>l15k4</id>
            <name>Jakub Liska</name>
            <email>liska.jakub@gmail.com</email>
          </developer>
        </developers>
  )

}