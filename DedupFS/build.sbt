libraryDependencies += "com.h2database" % "h2" % "1.4.196"

// Note: Eventually, hsqldb? and mysql? should be supported as well
// libraryDependencies += "org.hsqldb" % "hsqldb" % "2.3.3"

// TODO duplicated in FTPServer build
fullClasspath in Runtime += baseDirectory.value / "conf"
