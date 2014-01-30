version := "0.03-SNAPSHOT"

lazy val dedup = project.in(file("."))

lazy val davserver = project.dependsOn(dedup)

lazy val ftpserver = project.dependsOn(dedup)

libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.3.174",
  "org.specs2" %% "specs2" % "2.3.7" % "test"
)

(testOptions in Test) += Tests.Argument(TestFrameworks.Specs2, "html")

EclipseKeys.eclipseOutput := Some("bin")

EclipseKeys.withSource := true

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps"
)

XitrumPackage.copy()
