import gwi.sbt.CommonPlugin
import gwi.sbt.CommonPlugin.autoImport._

organization in ThisBuild := "net.globalwebindex"
fork in Test in ThisBuild := true
libraryDependencies in ThisBuild ++= loggingApi ++ akkaDeps ++ testingDeps ++ Seq(pprint)

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
