version := "1"
scalaVersion := "2.13.0"
scalacOptions := Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked", "-language:postfixOps")
resolvers += "bintray" at "http://jcenter.bintray.com"
libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.3"
libraryDependencies += "com.h2database" % "h2" % "1.4.199"

lazy val createApp = taskKey[Unit]("Create the app.")
createApp := {
  val jars = (Runtime / fullClasspathAsJars).value.map(_.data)
  val appDir = baseDirectory.value / "target" / "app"
  IO.delete(appDir)
  IO.copyDirectory(file("src/main/script"), appDir)
  (appDir / "dedup.sh").setExecutable(true)
  jars.foreach(file => IO.copyFile(file, appDir / "lib" / file.name))
  
  val hgRevision = try {
    import scala.sys.process._
    val parentEx = "parent: \\d*:([0-9a-f]*).*".r
    val hgSum = "hg sum".lineStream
    val revision = hgSum.collectFirst { case parentEx(rev) => rev }.get
    if (hgSum.contains("commit: (clean)")) s"-$revision" else s"-$revision+"
  } catch { case _: Throwable => "" }
  val date = new java.text.SimpleDateFormat("yyyy.MM.dd").format(new java.util.Date())
  IO.touch(appDir / s"$date$hgRevision.version")
}
