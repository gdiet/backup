lazy val dedupfs = project
  .in(file("."))
  .settings(
    name := "dedupfs",
    version := "current",
    scalaVersion := "3.1.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.10",
    libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.7",
    libraryDependencies += "com.h2database" % "h2" % "1.4.200", // Check compatibility before upgrading!
    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test",
  )

lazy val createApp = taskKey[Unit]("Create the app.")
createApp := {
  val jars = (Runtime / fullClasspathAsJars).value.map(_.data)
  val appDir = baseDirectory.value / "target" / "app"
  IO.delete(appDir)
  IO.copyDirectory(file("src/main/script"), appDir)
  (appDir / "dedup.sh").setExecutable(true)
  jars.foreach(file => IO.copyFile(file, appDir / "lib" / file.name))
  streams.value.log.info(s"Built dedup app")
}
