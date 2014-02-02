// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
version := "0.03-SNAPSHOT"


libraryDependencies ++= Seq (
  "org.apache.ftpserver" % "ftpserver-core" % "1.0.6",
  "ch.qos.logback" % "logback-classic" % "1.0.13"
)


// other settings

EclipseKeys.eclipseOutput := Some("bin")

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps"
)

XitrumPackage.copy()
