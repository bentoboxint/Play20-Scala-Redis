import sbt._
import Keys._
import PlayProject._

object ApplicationBuild extends Build {

    val appName         = "simpleRedisExample"
    val appVersion      = "1.0-SNAPSHOT"

    val appDependencies = Seq(
      // Add your project dependencies here,
      "com.redis" % "play20-scala-redis_2.9.1" % "0.1"
    )

    val main = PlayProject(appName, appVersion, appDependencies, mainLang = SCALA).settings(
      // Add your own project settings here
      //resolvers ++= Seq("Local Play Repo" at "file://usr/local/Cellar/play/2.0.2/libexec/repository/local")
      resolvers += Resolver.url("bentoboxint play repository", url("http://bentoboxint.github.com/releases/"))(Resolver.ivyStylePatterns)
    )

}
