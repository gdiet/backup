// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php

libraryDependencies ++= Seq (
  "org.apache.ftpserver" % "ftpserver-core" % "1.0.6" withSources(),
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

XitrumPackage.copy()
