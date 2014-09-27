// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
version in ThisBuild := "0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.2"

fork in ThisBuild := true

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:postfixOps,implicitConversions"
)

excludeFilter in (ThisBuild, Compile, unmanagedResources) := "logback.xml"

lazy val core = project

lazy val davserver = project dependsOn (core  % "test->test;compile->compile")

lazy val ftpserver = project dependsOn (core  % "test->test;compile->compile")

XitrumPackage.copy()
