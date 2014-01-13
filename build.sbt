lazy val dedup = project.in(file("."))

lazy val davserver = project.dependsOn(dedup)

libraryDependencies ++= Seq (
  "com.h2database" % "h2" % "1.3.174"
)

EclipseKeys.eclipseOutput := Some("bin")

EclipseKeys.withSource := true