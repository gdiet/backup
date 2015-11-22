version in ThisBuild := "0.4-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.7"

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

fork in run in ThisBuild := true
connectInput in run in ThisBuild := true
fork in test in ThisBuild := true

libraryDependencies in ThisBuild ++= Seq("org.specs2" %% "specs2-core" % "3.6.4" % "test")
scalacOptions in Test in ThisBuild ++= Seq("-Yrangepos")

lazy val All = project aggregate (Common, Logging, ByteStore, DedupFS, FTPServer)
lazy val Common = project
lazy val CommonTest = Common % "test->test"
lazy val Logging = project dependsOn Common
lazy val ByteStore = project dependsOn (Common, Logging, CommonTest)
lazy val DedupFS = project dependsOn (ByteStore, )
lazy val FTPServer = project dependsOn DedupFS
