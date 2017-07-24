version in ThisBuild := "0.5-SNAPSHOT"
scalaVersion in ThisBuild := "2.12.2"
scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)
libraryDependencies in ThisBuild ++= Seq("org.specs2" %% "specs2-core" % "3.9.2" % "test")

lazy val Common = project
lazy val DedupFS = project dependsOn Common
