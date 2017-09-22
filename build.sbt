
version in ThisBuild := "0.1.3"
crossScalaVersions in ThisBuild := Seq("2.12.3", "2.11.8")
organization in ThisBuild := "net.globalwebindex"
fork in Test in ThisBuild := true

libraryDependencies in ThisBuild ++= loggingApi

lazy val saturator = (project in file("."))
  .aggregate(`saturator-core`, `saturator-example`)

lazy val `saturator-core` = (project in file("core"))
  .enablePlugins(CommonPlugin)
  .settings(name := "saturator-core")
  .settings(libraryDependencies ++= Seq(asciiGraphs, akkaActor, akkaPersistence, akkaPersistenceRedis, akkaKryoSerialization, akkaTestkit, scalatest, loggingImplLog4j % "test"))
  .settings(publishSettings("GlobalWebIndex", "saturator-core", s3Resolver))

lazy val `saturator-example` = (project in file("example"))
  .enablePlugins(CommonPlugin, DockerPlugin)
  .settings(name := "saturator-example")
  .settings(libraryDependencies += loggingImplLog4j)
  .settings(assemblySettings("saturator-example", Some("gwi.saturator.Launcher")))
  .settings(deploySettings("java:8", "gwiq", "saturator-example", "gwi.saturator.Launcher"))
  .dependsOn(`saturator-core` % "compile->compile;test->test")
