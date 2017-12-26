
version in ThisBuild := "0.3.9"
crossScalaVersions in ThisBuild := Seq("2.12.4", "2.11.8")
organization in ThisBuild := "net.globalwebindex"
fork in Test in ThisBuild := true
libraryDependencies in ThisBuild ++= loggingApi

lazy val saturator = (project in file("."))
  .settings(aggregate in update := false)
  .settings(publish := { })
  .aggregate(`Saturator-api`, `Saturator-core`, `Saturator-example`)

lazy val `Saturator-api` = (project in file("api"))
  .enablePlugins(CommonPlugin)
  .settings(libraryDependencies ++= clist)
  .settings(publishSettings("GlobalWebIndex", "saturator-api", s3Resolver))

lazy val `Saturator-core` = (project in file("core"))
  .enablePlugins(CommonPlugin)
  .settings(publishSettings("GlobalWebIndex", "saturator-core", s3Resolver))
  .settings(libraryDependencies ++= Seq(
      asciiGraphs, akkaActor, akkaPersistence, akkaKryoSerialization,
      akkaTestkit, scalatest, akkaPersistenceInMemory % "test", loggingImplLog4j % "test"
    )
  ).dependsOn(`Saturator-api` % "compile->compile;test->test")

lazy val `Saturator-example` = (project in file("example"))
  .enablePlugins(CommonPlugin, DockerPlugin)
  .settings(publish := { })
  .settings(libraryDependencies ++= clist ++ Seq(akkaPersistenceDynamoDB, akkaPersistenceRedis, loggingImplLog4j))
  .settings(deploy(DeployDef(config("app") extend Compile, "openjdk:9", "gwiq", "saturator-example", "gwi.s8.Launcher")))
  .dependsOn(`Saturator-core` % "compile->compile;test->test")
