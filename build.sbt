
version in ThisBuild := "0.2.6"
crossScalaVersions in ThisBuild := Seq("2.12.3", "2.11.8")
organization in ThisBuild := "net.globalwebindex"
fork in Test in ThisBuild := true
libraryDependencies in ThisBuild ++= loggingApi

lazy val saturator = (project in file("."))
  .settings(aggregate in update := false)
  .settings(publish := { })
  .aggregate(`saturator-api`, `saturator-core`, `saturator-example`)

lazy val `saturator-api` = (project in file("api"))
  .enablePlugins(CommonPlugin)
  .settings(publishSettings("GlobalWebIndex", "saturator-api", s3Resolver))

lazy val `saturator-core` = (project in file("core"))
  .enablePlugins(CommonPlugin)
  .settings(publishSettings("GlobalWebIndex", "saturator-core", s3Resolver))
  .settings(libraryDependencies ++= Seq(
      asciiGraphs, akkaActor, akkaPersistence, akkaKryoSerialization,
      akkaTestkit, scalatest, akkaPersistenceInMemory % "test", loggingImplLog4j % "test"
    )
  ).dependsOn(`saturator-api` % "compile->compile;test->test")

lazy val `saturator-example` = (project in file("example"))
  .enablePlugins(CommonPlugin, DockerPlugin)
  .settings(publish := { })
  .settings(libraryDependencies ++= clist ++ Seq(akkaPersistenceDynamoDB, akkaPersistenceRedis, loggingImplLog4j))
  .settings(deploy(DeployDef(config("app") extend Compile, "openjdk:8", "gwiq", "saturator-example", "gwi.s8.Launcher")))
  .dependsOn(`saturator-core` % "compile->compile;test->test")
