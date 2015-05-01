import bintray.AttrMap
import bintray._

name := "zipkin"

organization := "com.twitter"

scalaVersion := "2.9.3"

publishMavenStyle := false

bintrayPublishSettings

bintray.Keys.repository in bintray.Keys.bintray := "sbt-plugins"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

bintray.Keys.bintrayOrganization in bintray.Keys.bintray := None