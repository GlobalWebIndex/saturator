logLevel := Level.Warn

resolvers ++= Seq(
  "Maven Central Google Mirror EU" at "https://maven-central-eu.storage-download.googleapis.com/repos/central/data/",
  Resolver.url("lustefaniak/sbt-plugins", url("https://dl.bintray.com/lustefaniak/sbt-plugins/"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.typesafe.sbt"     % "sbt-native-packager"   % "1.3.10")
addSbtPlugin("io.get-coursier"      % "sbt-coursier"          % "1.0.2")
addSbtPlugin("net.globalwebindex"   % "sbt-smaller-docker"    % "0.0.4")
addSbtPlugin("com.dwijnand"         % "sbt-dynver"            % "3.0.0")
addSbtPlugin("org.foundweekends"    % "sbt-bintray"           % "0.5.4")
addSbtPlugin("com.frugalmechanic"   % "fm-sbt-s3-resolver"    % "0.16.0")