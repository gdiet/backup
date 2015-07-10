libraryDependencies ++= Seq (
  "net.java.dev.jna" % "jna" % "4.1.0",
  "org.specs2" %% "specs2-core" % "3.6.2" % "test"
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
scalacOptions in Test ++= Seq("-Yrangepos")

fork in Test := true
javaOptions in Test += "-Djna.library.path=c/bin/Release"

XitrumPackage.copy()
