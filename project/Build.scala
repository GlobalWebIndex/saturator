import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin
import sbtdocker.DockerPlugin
import sbtdocker.DockerPlugin.autoImport._
import sbtdocker.mutable.Dockerfile

object Build extends sbt.Build {

  lazy val appVersion = "0.05-SNAPSHOT"

  lazy val akkaVersion = "2.4.11"

  lazy val testSettings = Seq(
    testOptions in Test += Tests.Argument("-oDFI"),
    fork in Test := true,
    initialCommands in (Test, console) := """ammonite.repl.Main().run()"""
  )

  lazy val libraryDeps = Seq(
    "com.lihaoyi"                 %%  "pprint"                                % "0.4.1",
    "com.typesafe.scala-logging"  %%  "scala-logging"                         % "3.4.0",
    "com.hootsuite"               %%  "akka-persistence-redis"                % "0.6.0",
    "com.github.romix.akka"       %%  "akka-kryo-serialization"               % "0.5.0",
    "com.typesafe.akka"           %%  "akka-remote"                           % akkaVersion,
    "com.typesafe.akka"           %%  "akka-cluster"                          % akkaVersion,
    "com.typesafe.akka"           %%  "akka-cluster-tools"                    % akkaVersion,
    "com.typesafe.akka"           %%  "akka-persistence"                      % akkaVersion,
    "com.typesafe.akka"           %%  "akka-testkit"                          % akkaVersion   % "test",
    "org.scalatest"               %%  "scalatest"                             % "3.0.0"       % "test",
    "com.lihaoyi"                 %   "ammonite-repl"                         % "0.7.7"       % "test" cross CrossVersion.full
  )

  lazy val sharedSettings = Seq(
    organization := "net.globalwebindex",
    version := appVersion,
    scalaVersion := "2.11.8",
    offline := true,
    assembleArtifact := false,
    scalacOptions ++= Seq(
      "-unchecked", "-deprecation", "-feature", "-Xfatal-warnings",
      "-Xlint", "-Xfuture",
      "-Yinline-warnings", "-Ywarn-adapted-args", "-Ywarn-inaccessible",
      "-Ywarn-nullary-override", "-Ywarn-nullary-unit", "-Yno-adapted-args"
    ),
    libraryDependencies ++= libraryDeps,
    autoCompilerPlugins := true,
    cancelable in Global := true,
    resolvers ++= Seq(
      Resolver.sonatypeRepo("snapshots"),
      Resolver.typesafeRepo("releases"),
      Resolver.mavenLocal
    )
  ) ++ testSettings


  val publishSettings = Seq(
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    publishTo := Some("S3 Snapshots" at "s3://maven.globalwebindex.net.s3-website-eu-west-1.amazonaws.com/snapshots"),
    pomExtra :=
      <url>https://github.com/GlobalWebIndex/saturator</url>
        <licenses>
          <license>
            <name>The MIT License (MIT)</name>
            <url>http://opensource.org/licenses/MIT</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:GlobalWebIndex/saturator.git</url>
          <connection>scm:git:git@github.com:GlobalWebIndex/saturator.git</connection>
        </scm>
        <developers>
          <developer>
            <id>l15k4</id>
            <name>Jakub Liska</name>
            <email>jakub@globalwebindex.net</email>
          </developer>
        </developers>
  )

  val workingDir = SettingKey[File]("working-dir", "Working directory path for running applications")
  def assemblySettings(appName: String, mainClassFqn: Option[String]) = {
    Seq(
      assembleArtifact := true,
      assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false),
      assemblyJarName in assembly := s"$appName.jar",
      assemblyJarName in assemblyPackageDependency := s"$appName-deps.jar",
      workingDir := baseDirectory.value / "deploy",
      cleanFiles += baseDirectory.value / "deploy" / "bin",
      baseDirectory in run := workingDir.value,
      baseDirectory in runMain := workingDir.value,
      test in assembly := {},
      test in assemblyPackageDependency := {},
      mainClass in assembly := mainClassFqn, // Note that sbt-assembly cannot assemble jar with multiple main classes use SBT instead
      aggregate in assembly := false,
      aggregate in assemblyPackageDependency := false,
      assemblyOutputPath in assembly := workingDir.value / "bin" / (assemblyJarName in assembly).value,
      assemblyOutputPath in assemblyPackageDependency := workingDir.value / "bin" / (assemblyJarName in assemblyPackageDependency).value
    )
  }

  def deploySettings(baseImageName: String, repoName: String, appName: String, mainClassFqn: String, extraClasspath: Option[String] = None) = {
    Seq(
      docker <<= (docker dependsOn(assembly, assemblyPackageDependency)),
      dockerfile in docker :=
        new Dockerfile {
          from(baseImageName)
          run("/bin/mkdir", s"/opt/$appName")
          DockerUtils.dockerCopySorted(workingDir.value.absolutePath, s"/opt/$appName") { case (sourcePath, targetPath) => copy(new File(sourcePath), new File(targetPath)) }
          workDir(s"/opt/$appName")
          entryPoint("java", "-cp", s"bin/*" + extraClasspath.map(":" + _).getOrElse(""), mainClassFqn)
        },
      imageNames in docker := Seq(
        ImageName(s"$repoName/$appName:${version.value}"),
        ImageName(s"$repoName/$appName:latest")
      )
    )
  }

  lazy val core = (project in file("core"))
    .enablePlugins(DockerPlugin, BuildInfoPlugin)
    .settings(name := "saturator")
    .settings(sharedSettings)
    .settings(publishSettings)

  lazy val example = (project in file("example"))
    .enablePlugins(DockerPlugin)
    .settings(name := "saturator-example")
    .settings(sharedSettings)
    .settings(assemblySettings("saturator-example", Some("example.Launcher")))
    .settings(deploySettings("java:8", "gwiq", "saturator-example", "gwi.saturator.Launcher"))
    .settings(libraryDependencies ++= Seq(
      "org.backuity.clist"          %%  "clist-core"                            % "3.2.2",
      "org.backuity.clist"          %%  "clist-macros"                          % "3.2.2"       % "provided"
    )).dependsOn(core % "compile->compile;compile->test")

}
