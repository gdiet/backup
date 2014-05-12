// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php

scalaVersion := "2.10.4"

libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.3.176",
  "com.typesafe.slick" %% "slick" % "2.0.1",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.specs2" %% "specs2" % "2.3.11" % "test"
)

(testOptions in Test) += Tests.Argument(TestFrameworks.Specs2, "html", "console")

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps,implicitConversions"
)
