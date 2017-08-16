
version in ThisBuild := "0.0.7"
crossScalaVersions in ThisBuild := Seq("2.12.3", "2.11.8")
organization in ThisBuild := "net.globalwebindex"
fork in Test in ThisBuild := true

lazy val saturator = (project in file("."))
  .aggregate(`saturator-api`, `saturator-core`, `saturator-example`)

lazy val `saturator-api` = (project in file("api"))
  .enablePlugins(CommonPlugin)
  .settings(name := "saturator-api")
  .settings(publishSettings("GlobalWebIndex", "saturator-api", s3Resolver))

lazy val `saturator-core` = (project in file("core"))
  .enablePlugins(CommonPlugin)
  .settings(name := "saturator-core")
  .settings(libraryDependencies ++= loggingApi ++ Seq(akkaActor, akkaPersistence, akkaPersistenceRedis, akkaKryoSerialization, akkaTestkit, scalatest))
  .settings(publishSettings("GlobalWebIndex", "saturator-core", s3Resolver))
  .dependsOn(`saturator-api` % "compile->compile;test->test")

lazy val `saturator-example` = (project in file("example"))
  .enablePlugins(CommonPlugin, DockerPlugin)
  .settings(name := "saturator-example")
  .settings(assemblySettings("saturator-example", Some("gwi.saturator.Launcher")))
  .settings(deploySettings("java:8", "gwiq", "saturator-example", "gwi.saturator.Launcher"))
  .dependsOn(`saturator-core` % "compile->compile;test->test")
