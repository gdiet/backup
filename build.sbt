version in ThisBuild := "0.6-SNAPSHOT"
scalaVersion in ThisBuild := "2.12.7"
scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

lazy val DedupFS = project
  .in(file("."))
  .dependsOn(FuseFS)

// ---- subprojects ----

lazy val FuseFS = project
  .dependsOn(ScalaFS, MetaH2)
  .settings(
    resolvers += "bintray" at "http://jcenter.bintray.com",
    libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.2.1" // 0.5.2.1 as of 2018-07-25
  )

lazy val MetaH2 = project
  .dependsOn(Util)
  .settings(
    libraryDependencies += "com.h2database" % "h2" % "1.4.197" // 1.4.197 as of 2018-03-18
  )

lazy val ScalaFS = project
  .dependsOn(Util)

lazy val Util = project
  .settings(
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3" // 1.2.3 as of 2017-03-31
  )
