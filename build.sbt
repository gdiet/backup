// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
version := "0.03-SNAPSHOT"

lazy val dedup = project.in(file("."))

lazy val davserver = project.dependsOn(dedup)

lazy val ftpserver = project.dependsOn(dedup)

resolvers += "Sonatype Releases"  at "http://oss.sonatype.org/content/repositories/releases"

libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.3.175",
  "com.typesafe.slick" %% "slick" % "2.0.0",
  "com.mchange" % "c3p0" % "0.9.2.1",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "org.specs2" %% "specs2" % "2.3.7" % "test"
)

(testOptions in Test) += Tests.Argument(TestFrameworks.Specs2, "html")

EclipseKeys.eclipseOutput := Some("bin")

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps"
)

XitrumPackage.copy()
