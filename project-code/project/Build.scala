import sbt._
import Keys._
import PlayProject._

object ApplicationBuild extends Build {

    val appName         = "play20-scala-redis"
    val appVersion      = "0.2"

    val appDependencies = Seq(
        // Add your project dependencies here,
        "commons-pool" % "commons-pool" % "1.6",
        "junit"          % "junit"         % "4.8.1"  % "test",
        "org.scalatest"  % "scalatest_2.9.1" % "1.6.1" % "test",
        "com.twitter"    % "util_2.9.1"    % "1.12.13" % "test" intransitive(),
        "com.twitter"    % "finagle-core_2.9.1"  % "4.0.2" % "test"
    )

    val main = PlayProject(appName, appVersion, appDependencies, mainLang = SCALA).settings(
      // Add your own project settings here
      resolvers ++= Seq("release" at "http://maven.twttr.com"),
      organization := "com.redis"
    )

}
