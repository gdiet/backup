version in ThisBuild := "0.5-SNAPSHOT"
scalaVersion in ThisBuild := "2.12.2"
scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

lazy val Common = project
lazy val CommonDedup = project
lazy val Meta = project
lazy val MetaH2 = project dependsOn (Common, CommonDedup, Meta)
lazy val CoreDedup = project dependsOn MetaH2
lazy val DedupFS = project dependsOn CoreDedup
