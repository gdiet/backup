version in ThisBuild := "0.6-SNAPSHOT"
scalaVersion in ThisBuild := "2.12.6"
scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

lazy val Main = project dependsOn FuseFS
lazy val FuseFS = project
