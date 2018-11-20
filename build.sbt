import Dependencies._
import Deploy._

lazy val s3Resolver = "S3 Snapshots" at "s3://public.maven.globalwebindex.net.s3-eu-west-1.amazonaws.com/snapshots"

crossScalaVersions in ThisBuild := Seq("2.12.6", "2.11.8")
organization in ThisBuild := "net.globalwebindex"
fork in Test in ThisBuild := true
libraryDependencies in ThisBuild ++= loggingApi
resolvers in ThisBuild ++= Seq(
  "Maven Central Google Mirror EU" at "https://maven-central-eu.storage-download.googleapis.com/repos/central/data/",
  s3Resolver
)
version in ThisBuild ~= (_.replace('+', '-'))
dynver in ThisBuild ~= (_.replace('+', '-'))
cancelable in ThisBuild := true

lazy val `saturator-api` = (project in file("api"))
  .settings(libraryDependencies ++= clist)
  .settings(stage in Docker := null)
  .settings(publishSettings("GlobalWebIndex", "saturator-api", s3Resolver))

lazy val `saturator-core` = (project in file("core"))
  .settings(publishSettings("GlobalWebIndex", "saturator-core", s3Resolver))
  .settings(stage in Docker := null)
  .settings(libraryDependencies ++= Seq(
      asciiGraphs, akkaActor, akkaPersistence, akkaKryoSerialization, akkaSlf4j,
      akkaTestkit, scalatest, akkaPersistenceInMemory, loggingImplLogback % "test",
    )
  ).dependsOn(`saturator-api` % "compile->compile;test->test")

lazy val `saturator-example` = (project in file("example"))
  .enablePlugins(DockerPlugin, SmallerDockerPlugin, JavaAppPackaging)
  .settings(skip in publish := true)
  .settings(libraryDependencies ++= clist ++ Seq(akkaPersistenceDynamoDB, akkaPersistenceRedis, loggingImplLogback))
  .settings(Deploy.settings("gwiq", "saturator-example", "gwi.s8.Launcher"))
  .dependsOn(`saturator-core` % "compile->compile;test->test")
