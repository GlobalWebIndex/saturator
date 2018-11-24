import sbt._

object Dependencies {

  val akkaVersion                       = "2.5.16"

  lazy val clist                        = Seq(
    "org.backuity.clist"            %%    "clist-core"                                % "3.5.0",
    "org.backuity.clist"            %%    "clist-macros"                              % "3.5.0"                 % "provided"
  )
  lazy val loggingApi                   = Seq(
    "org.slf4j"                     %     "slf4j-api"                                 % "1.7.25",
    "com.typesafe.scala-logging"    %%    "scala-logging"                             % "3.9.0"
  )

  lazy val loggingImplLogback           = "ch.qos.logback"                %     "logback-classic"                    % "1.2.3"

  lazy val akkaActor                    = "com.typesafe.akka"             %%    "akka-actor"                         % akkaVersion
  lazy val akkaPersistence              = "com.typesafe.akka"             %%    "akka-persistence"                   % akkaVersion
  lazy val akkaPersistenceInMemory      = "com.github.dnvriend"           %%    "akka-persistence-inmemory"          % "2.5.1.1"               % "test"
  lazy val akkaPersistenceDynamoDB      = "com.typesafe.akka"             %%    "akka-persistence-dynamodb"          % "1.1.0"
  lazy val akkaPersistenceRedis         = "com.safety-data"               %%    "akka-persistence-redis"             % "0.3.0"
  lazy val akkaKryoSerialization        = "net.globalwebindex"            %%    "akka-kryo-serialization"            % "0.5.4" // adhoc published before PR is merged https://github.com/romix/akka-kryo-serialization/pull/124
  lazy val akkaSlf4j                    = "com.typesafe.akka"             %%    "akka-slf4j"                         % akkaVersion
  lazy val akkaTestkit                  = "com.typesafe.akka"             %%    "akka-testkit"                       % akkaVersion             % "test"

  lazy val asciiGraphs                  = "com.github.mdr"                %%    "ascii-graphs"                       % "0.0.7"
  lazy val scalatest                    = "org.scalatest"                 %%    "scalatest"                          % "3.0.5"                 % "test"

}
