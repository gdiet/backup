version in ThisBuild := "0.6-SNAPSHOT"
scalaVersion in ThisBuild := "2.12.6"
scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

lazy val DedupFS = project
  .in(file("."))
  .dependsOn(FuseFS)

lazy val FuseFS = project
  .dependsOn(ScalaFS)
  .settings(
    resolvers += "bintray" at "http://jcenter.bintray.com",
    libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.2.1"
  )

lazy val ScalaFS = project
