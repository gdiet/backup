lazy val dedupfs = project
  .in(file("."))
  .settings(
    name := "dedupfs",
    version := "0.1.0",
    scalaVersion := "3.0.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
  )
