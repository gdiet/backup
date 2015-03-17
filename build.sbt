version in ThisBuild := "0.3-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.6"

fork in ThisBuild := true

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:postfixOps"
)

lazy val backup = project in file(".") dependsOn (core, ftpserver, davserver)

lazy val core = project

lazy val ftpserver = project dependsOn core

lazy val davserver = project dependsOn core

XitrumPackage.copy()
