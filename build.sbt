version := "1"
scalaVersion := "2.13.1"
scalacOptions := Seq("-target:11", "-deprecation", "-feature", "-unchecked")
resolvers += "bintray" at "https://jcenter.bintray.com"
libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.3"
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
  
  val gitRevision = try {
    import scala.sys.process._
    val commitEx = "commit ([0-9a-f]{8}).*".r
    val revision = "git log -1".lineStream.collectFirst { case commitEx(rev) => rev }.get
    val clean = "git status".lineStream.exists(_.matches(".*working tree clean.*"))
    if (clean) revision else revision + "+"
  } catch { case _: Throwable => "xx" }
  val date = new java.text.SimpleDateFormat("yyyy.MM.dd").format(new java.util.Date())
  IO.touch(appDir / s"$date-$gitRevision.version")
  streams.value.log.info(s"Created dedup app $date-$gitRevision")
}
