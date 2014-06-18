// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php

scalaVersion := "2.11.1"

libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.4.178",
  "com.typesafe.slick" %% "slick" % "2.1.0-M2",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.specs2" %% "specs2" % "2.3.12" % "test"
)

(testOptions in Test) += Tests.Argument(TestFrameworks.Specs2, "html", "console")

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps,implicitConversions"
)
