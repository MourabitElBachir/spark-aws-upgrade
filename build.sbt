enablePlugins(JavaAppPackaging)
enablePlugins(UniversalPlugin)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case "git.properties" => MergeStrategy.discard
    case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf" => MergeStrategy.concat
    case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
    case PathList("org", "apache", xs@_*) => MergeStrategy.last
    case PathList("com", "google", xs@_*) => MergeStrategy.last
    case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
    case PathList("javax", "inject", xs@_*) => MergeStrategy.last
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)


lazy val commonSettings = Seq(
  organization := "com.spark.tech.test",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.8",
  scalacOptions := Seq(
    "-deprecation",
    "-feature"
  ),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0",
    "org.apache.spark" %% "spark-sql" % "2.4.0",
    "org.apache.spark" %% "spark-mllib" % "2.4.0",
    "mysql" % "mysql-connector-java" % "5.1.46" % "provided",
    "org.scalactic" %% "scalactic" % "3.2.0",
    "org.scalatest" %% "scalatest" % "3.2.0" % "test"
  ),
  test in assembly := {}
)

//PROJECTS
lazy val root = project
  .in(file("."))
  .aggregate(commonUtils, roadTraffic, airQuality)
  .dependsOn(roadTraffic, airQuality)
  .settings(
    name := "root",
    commonSettings,
    assemblySettings
  )


//MODULES

//COMMON
lazy val commonUtils = project
  .in(file("common-utils"))
  .settings(
    name := "common-utils",
    commonSettings,
    assemblySettings
  )

lazy val roadTraffic = project
  .in(file("road-traffic"))
  .dependsOn(commonUtils)
  .settings(
    name := "road-traffic",
    commonSettings,
    assemblySettings
  )

lazy val airQuality = project
  .in(file("air-quality"))
  .dependsOn(commonUtils)
  .settings(
    name := "air-quality",
    commonSettings,
    assemblySettings
  )





