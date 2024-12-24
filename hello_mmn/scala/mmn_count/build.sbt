import Dependencies._

ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "1.0"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "mmn_count"

lazy val root = (project in file("."))
  .settings(
    name := "mmn_count",
    scalacOptions += "-deprecation",
    libraryDependencies ++= Seq(
      munit % Test,
      spark_sql
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
