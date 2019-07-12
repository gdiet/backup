version := new java.text.SimpleDateFormat("yyyy.MM.dd").format(new java.util.Date())
scalaVersion := "2.13.0"
scalacOptions := Seq("-target:jvm-1.8")
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
}
