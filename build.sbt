lazy val dedupfs = project
  .in(file("."))
  .settings(
    name := "dedupfs",
    version := "current",
    scalaVersion := "3.1.3",
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.4.1",
    libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.7",
    // Update dedup.db.H2.dbName accordingly when updating H2
    // to a version with incompatible binary storage format.
    // Document migration, similar to 4.x -> 5.x migration.
    libraryDependencies += "com.h2database" % "h2" % "2.1.214",
    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.13" % "test",
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
