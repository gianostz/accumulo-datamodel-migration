ThisBuild / scalaVersion := "3.3.3"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "org.example"

lazy val root = (project in file("."))
  .settings(
    name := "accumulo-migration-poc",
    libraryDependencies ++= Seq(
      "org.apache.accumulo" % "accumulo-core"        % "2.1.2",
      "org.apache.accumulo" % "accumulo-minicluster" % "2.1.2",
      "org.apache.hadoop"   % "hadoop-common"        % "3.3.6",
      "ch.qos.logback"      % "logback-classic"      % "1.4.14",
    ),
    // Fork so MiniAccumuloCluster gets a clean JVM with the full classpath
    fork             := true,
    run / javaOptions ++= Seq("-Xmx512m", "-Djava.net.preferIPv4Stack=true"),
  )
