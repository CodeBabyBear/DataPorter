name := "DataPorter"

version := "1.0"

scalaVersion := "2.11.11"

import sbt.Keys._
import sbt._

lazy val baseversion = "0.38"
lazy val cdhVersion = "cdh5.8.3"
lazy val sparkVersion = "2.1.1"

coverageEnabled := false
lazy val defaultSettings =
  Defaults.defaultSettings ++ Seq(
    version := baseversion,
    scalaVersion := "2.11.11",
    organization := "com.newegg.eims",
    publishMavenStyle := true,
    autoAPIMappings := true,
    externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false),
    resolvers ++= Seq(
    ),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.3.0" % "test"
    )
  ) ++ scoverage.ScoverageSbtPlugin.projectSettings

lazy val database = Project(id = "Base", base = file("Base"), settings = defaultSettings ++ Seq(
  name := "DataPorter.Base",
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % "2.11.11"
  )
))

//lazy val orc = Project(id = "Orc", base = file("Orc"), settings = defaultSettings ++ Seq(
//  name := "DataPorter.Orc",
//  libraryDependencies ++= Seq("org.apache.orc" % "orc-core" % "1.2.1")
//)) dependsOn database

lazy val csv = Project(id = "Csv", base = file("Csv"), settings = defaultSettings ++ Seq(
  name := "DataPorter.Csv",
  libraryDependencies ++= Seq("org.apache.commons" % "commons-csv" % "1.4")
)) dependsOn database

lazy val hdfs = Project(id = "HDFS", base = file("HDFS"), settings = defaultSettings ++ Seq(
  name := "DataPorter.HDFS",
  version := baseversion + "-" + ("2.6.0-" + cdhVersion),
  libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-client" % ("2.6.0-" + cdhVersion))
))

lazy val hive = Project(id = "Hive", base = file("Hive"), settings = defaultSettings ++ Seq(
  name := "DataPorter.Hive",
  libraryDependencies ++= Seq("com.newegg.eims" %% "hiveclient" % "1.2")
)) dependsOn database

lazy val parquet = Project(id = "Parquet", base = file("Parquet"), settings = defaultSettings ++ Seq(
  name := "DataPorter.Parquet",
  version := baseversion + "-" + sparkVersion,
  libraryDependencies ++= Seq(
    "com.twitter" % "parquet-hadoop" % ("1.5.0-" + cdhVersion),
    "org.apache.hadoop" % "hadoop-client" % ("2.6.0-" + cdhVersion) excludeAll ExclusionRule("javax.servlet", "servlet-api") ,
    "org.apache.spark" %% "spark-sql" % sparkVersion % "test"
  )
)) dependsOn database

lazy val root = project.in(file(".")).aggregate(database, csv, hdfs, hive, parquet).settings(
  aggregate in update := true,
  publishLocal := {},
  publish := {}
).enablePlugins(ScalaUnidocPlugin)