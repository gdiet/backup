// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
version := "0.03-SNAPSHOT"

lazy val dedup = project.in(file("."))

lazy val davserver = project.dependsOn(dedup)

lazy val ftpserver = project.dependsOn(dedup)

libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.3.175",
  "org.specs2" %% "specs2" % "2.3.10" % "test"
)

(testOptions in Test) += Tests.Argument(TestFrameworks.Specs2, "html")

EclipseKeys.eclipseOutput := Some("bin")

EclipseKeys.withSource := true

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps"
)

XitrumPackage.copy()
