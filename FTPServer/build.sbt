libraryDependencies += "org.apache.ftpserver" % "ftpserver-core" % "1.0.6" withSources()

fullClasspath in Runtime += baseDirectory.value / "conf"
