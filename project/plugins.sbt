resolvers += "S3 Snapshots" at "s3://public.maven.globalwebindex.net.s3-website-eu-west-1.amazonaws.com/snapshots"
addSbtPlugin("net.globalwebindex" % "sbt-common" % "0.1-SNAPSHOT")

// for development, use :
// dependsOn(ProjectRef(uri("file:///home/ubuntu/src/sbt-common"), "sbt-common"))
// dependsOn(ProjectRef(uri("ssh://git@github.com/l15k4/sbt-common.git"), "sbt-common"))