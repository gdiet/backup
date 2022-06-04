lazy val dedupfs = project
  .in(file("."))
  .settings(
    name := "dedupfs",
    version := "current",
    scalaVersion := "3.1.1",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.10",
    libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.7",
    // Update dedup.db.H2.dbName accordingly when updating H2 version.
    // Document migration, similar to 4.x -> 5.x migration.
    libraryDependencies += "com.h2database" % "h2" % "2.1.210",
    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % "test",
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
