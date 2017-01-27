import gwi.sbt.CommonPlugin
import gwi.sbt.CommonPlugin.autoImport._

lazy val s3SnapshotResolver = "S3 Snapshots" at "s3://public.maven.globalwebindex.net.s3-website-eu-west-1.amazonaws.com/snapshots"

organization in ThisBuild := "net.globalwebindex"
version in ThisBuild  := "0.06-SNAPSHOT"
resolvers in ThisBuild  += s3SnapshotResolver
fork in Test in ThisBuild := true
libraryDependencies in ThisBuild ++= loggingApi ++ akkaDeps ++ testingDeps ++ Seq(pprint)

lazy val saturator = (project in file("."))
  .aggregate(core, example)

lazy val core = (project in file("core"))
  .enablePlugins(CommonPlugin)
  .settings(name := "saturator")
  .settings(publishSettings("GlobalWebIndex", "saturator", s3SnapshotResolver))

lazy val example = (project in file("example"))
  .enablePlugins(CommonPlugin, DockerPlugin)
  .settings(name := "saturator-example")
  .settings(assemblySettings("saturator-example", Some("example.Launcher")))
  .settings(deploySettings("java:8", "gwiq", "saturator-example", "gwi.saturator.Launcher"))
  .dependsOn(core % "compile->compile;compile->test")
