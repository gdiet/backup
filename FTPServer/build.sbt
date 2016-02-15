libraryDependencies += "org.apache.ftpserver" % "ftpserver-core" % "1.0.6" withSources()
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.5"

fullClasspath in Runtime += baseDirectory.value / "conf"
