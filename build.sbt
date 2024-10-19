val H2_VERSION = "2.3.232" // since dedupfs 6.0
val PREVIOUS_H2_VERSION = "2.1.214" // used by dedupfs 5.x

lazy val dedupfs = project
  .in(file("."))
  .settings(
    name := "dedupfs",
    version := "current",
    scalaVersion := "3.3.3", // 3.3.x is LTS
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    // logback 1.5.7 brings an unexpected message to stderr,
    // see https://github.com/qos-ch/slf4j/issues/422
    // still present in 1.5.11, so let's wait ...
    // see https://github.com/qos-ch/logback/blob/ea3cec87a154efec8d8f62a312020827d88b11dc/pom.xml#L77
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.6",
    libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.8",
    // Update dedup.db.H2.dbName accordingly when updating H2
    // to a version with incompatible binary storage format.
    // Document migration, similar to 4.x -> 5.x migration.
    libraryDependencies += "com.h2database" % "h2" % H2_VERSION,
    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  )

lazy val createApp = taskKey[Unit]("Create the app.")
createApp := {
  val appDir = baseDirectory.value / "target" / "app"
  IO.delete(appDir)

  IO.copyDirectory(file("src/main/script"), appDir)

  val jars = (Runtime / fullClasspathAsJars).value.map(_.data)
  val h2jarName = s"h2-$H2_VERSION.jar"
  val (h2jars, libraries) = jars.partition(_.name == h2jarName)
  require(h2jars.length == 1, s"H2 jar $h2jarName not found or more than one: $h2jars")
  h2jars.foreach(file => IO.copyFile(file, appDir / "lib-h2" / file.name))
  libraries.foreach(file => IO.copyFile(file, appDir / "lib" / file.name))

  val previousH2jarName = s"h2-$PREVIOUS_H2_VERSION.jar"
  val previousH2url = url(s"https://repo1.maven.org/maven2/com/h2database/h2/$PREVIOUS_H2_VERSION/$previousH2jarName")
  IO.createDirectory(appDir / "lib-h2-previous")
  import scala.sys.process.*
  previousH2url #> (appDir / "lib-h2-previous" / previousH2jarName) !

  streams.value.log.info(s"Built dedup app")
}
