name := "zipkin"

organization := "com.twitter"

scalaVersion := "2.9.3"

publishMavenStyle := false

version := "1.2.01"

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

// bintray values below only affect the top level aggregator project, check Project.scala for default settings per sub project

bintrayOrganization := Some("lookout")

bintrayRepository := "zipkin"
