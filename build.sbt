lazy val dedupfs = project
  .in(file("."))
  .settings(
    name := "dedupfs",
    version := "current",
    scalaVersion := "3.3.3", // 3.3.x is LTS
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    // logback 1.5.7 brings an unexpected message to stderr,
    // see https://github.com/qos-ch/slf4j/issues/422
    // so let's wait for 1.5.8 ...
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.6",
    libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.7",
    // Update dedup.db.H2.dbName accordingly when updating H2
    // to a version with incompatible binary storage format.
    // Document migration, similar to 4.x -> 5.x migration.
    libraryDependencies += "com.h2database" % "h2" % "2.1.214",
    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  )

lazy val createApp = taskKey[Unit]("Create the app.")
createApp := {
  val jars = (Runtime / fullClasspathAsJars).value.map(_.data)
  val appDir = baseDirectory.value / "target" / "app"
  IO.delete(appDir)
  IO.copyDirectory(file("src/main/script"), appDir)
  jars.foreach(file => IO.copyFile(file, appDir / "lib" / file.name))
  streams.value.log.info(s"Built dedup app")
}
