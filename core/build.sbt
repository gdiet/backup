// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php

libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.4.181",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.specs2" %% "specs2" % "2.4.1" % "test"
)

(testOptions in Test) += Tests.Argument(TestFrameworks.Specs2, "html", "console")

// FIXME should be set globally
XitrumPackage.copy()
