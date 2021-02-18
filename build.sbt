version := "current"
scalaVersion := "2.13.4"
scalacOptions := Seq("-target:11", "-deprecation", "-feature", "-unchecked")
libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.5"
libraryDependencies += "com.h2database" % "h2" % "1.4.200" // Check compatibility before upgrading!
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

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
