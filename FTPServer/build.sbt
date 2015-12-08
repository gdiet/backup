libraryDependencies += "org.apache.ftpserver" % "ftpserver-core" % "1.0.6"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3"

fullClasspath in Runtime += baseDirectory.value / "conf"
