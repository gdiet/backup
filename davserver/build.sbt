// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php

// Add milton webdav (see http://milton.io).
// Milton uses slf4j as logging frontend. As logging backend, use logback
// (see http://www.slf4j.org/manual.html and http://logback.qos.ch).

resolvers += "ettrema-repo" at "http://milton.io/maven"

libraryDependencies ++= Seq (
  "io.milton" % "milton-api" % "2.6.2.4" withSources(),
  "io.milton" % "milton-server-ce" % "2.6.2.4" withSources(),
  "commons-lang" % "commons-lang" % "2.6", // Needed but not included automatically
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

// Add jetty server (see http://www.eclipse.org/jetty).

libraryDependencies ++= Seq (
  "org.eclipse.jetty" % "jetty-server" % "9.2.2.v20140723",
  "org.eclipse.jetty" % "jetty-servlet" % "9.2.2.v20140723"
)

XitrumPackage.copy()
