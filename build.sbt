scalaVersion in ThisBuild := "2.13.0-M5"
scalacOptions in ThisBuild ++= Seq("-deprecation", "-feature", "-unchecked")

libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.2.1" // 0.5.2.1 as of 2018-07-25
libraryDependencies += "com.h2database" % "h2" % "1.4.197" // 1.4.197 as of 2018-03-18
