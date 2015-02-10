// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
version in ThisBuild := "0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.5"

fork in ThisBuild := true

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:postfixOps"
)

libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.4.185",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.specs2" %% "specs2" % "2.4.16" % "test"
)

XitrumPackage.copy()
