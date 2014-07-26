// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
version in ThisBuild := "0.10-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.1"

fork in Test in ThisBuild := true

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:postfixOps,implicitConversions"
)

lazy val core = project

lazy val davserver = project dependsOn (core  % "test->test;compile->compile")

XitrumPackage.copy()
