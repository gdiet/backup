version in ThisBuild := "0.4-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.7"

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

libraryDependencies in ThisBuild ++= Seq("org.specs2" %% "specs2-core" % "3.6.4" % "test")
scalacOptions in Test in ThisBuild ++= Seq("-Yrangepos")

lazy val Common = project
lazy val ByteStore = project dependsOn (Common % "compile->compile;test->test")
lazy val DedupFS = project dependsOn ByteStore
