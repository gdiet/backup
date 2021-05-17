lazy val dedupfs = project
  .in(file("."))
  .settings(
    name := "dedupfs",
    version := "0.1.0",
    scalaVersion := "3.0.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
    libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.5",
    libraryDependencies += "com.h2database" % "h2" % "1.4.200" // Check compatibility before upgrading!
)
