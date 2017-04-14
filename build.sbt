
version in ThisBuild := "0.0.2"
crossScalaVersions in ThisBuild := Seq("2.12.1", "2.11.8")
organization in ThisBuild := "net.globalwebindex"
fork in Test in ThisBuild := true
libraryDependencies in ThisBuild ++= loggingApi ++ Seq(akkaActor, akkaTestkit, akkaPersistence, akkaPersistenceRedis, akkaKryoSerialization, scalatest, pprint)

lazy val saturator = (project in file("."))
  .aggregate(`saturator-core`, `saturator-example`)

lazy val `saturator-core` = (project in file("core"))
  .enablePlugins(CommonPlugin)
  .settings(name := "saturator-core")
  .settings(publishSettings("GlobalWebIndex", "saturator", s3Resolver))

lazy val `saturator-example` = (project in file("example"))
  .enablePlugins(CommonPlugin, DockerPlugin)
  .settings(name := "saturator-example")
  .settings(assemblySettings("saturator-example", Some("gwi.saturator.Launcher")))
  .settings(deploySettings("java:8", "gwiq", "saturator-example", "gwi.saturator.Launcher"))
  .dependsOn(`saturator-core` % "compile->compile;compile->test")
