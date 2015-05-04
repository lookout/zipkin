import bintray.AttrMap
import bintray._

name := "zipkin"

organization := "com.twitter"

scalaVersion := "2.9.3"

publishMavenStyle := false

seq(bintraySettings:_*)

bintrayPublishSettings

version := "1.2.0"

bintray.Keys.repository in bintray.Keys.bintray := "sbt-plugins"

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

bintray.Keys.repository in bintray.Keys.bintray := "zipkin"

bintray.Keys.bintrayOrganization in bintray.Keys.bintray := Some("lookout")