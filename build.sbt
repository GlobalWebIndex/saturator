import Dependencies._
import Deploy._

crossScalaVersions in ThisBuild := Seq("2.12.6", "2.11.8")
organization in ThisBuild := "net.globalwebindex"
fork in Test in ThisBuild := true
libraryDependencies in ThisBuild ++= loggingApi
resolvers in ThisBuild ++= Seq(
  "Maven Central Google Mirror EU" at "https://maven-central-eu.storage-download.googleapis.com/repos/central/data/",
  Resolver.bintrayRepo("l15k4", "GlobalWebIndex")
)
version in ThisBuild ~= (_.replace('+', '-'))
dynver in ThisBuild ~= (_.replace('+', '-'))
cancelable in ThisBuild := true
publishArtifact in ThisBuild := false
stage in (ThisBuild, Docker) := null

lazy val `saturator-api` = (project in file("api"))
  .settings(libraryDependencies ++= clist)
  .settings(bintraySettings("GlobalWebIndex", "saturator"))

lazy val `saturator-core` = (project in file("core"))
  .settings(bintraySettings("GlobalWebIndex", "saturator"))
  .settings(libraryDependencies ++= Seq(
      asciiGraphs, akkaActor, akkaPersistence, akkaKryoSerialization, akkaSlf4j,
      akkaTestkit, scalatest, akkaPersistenceInMemory, loggingImplLogback % "test"
    )
  ).dependsOn(`saturator-api` % "compile->compile;test->test")

lazy val `saturator-example` = (project in file("example"))
  .enablePlugins(DockerPlugin, SmallerDockerPlugin, JavaAppPackaging)
  .settings(libraryDependencies ++= clist ++ Seq(akkaPersistenceDynamoDB, akkaPersistenceRedis, loggingImplLogback))
  .settings(dockerSettings("gwiq", "saturator-example", "gwi.s8.Launcher"))
  .dependsOn(`saturator-core` % "compile->compile;test->test")
