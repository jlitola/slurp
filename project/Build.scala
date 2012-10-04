import sbt._
import Keys._
import PlayProject._

object ApplicationBuild extends Build {

    val appName         = "crawler"
    val appVersion      = "1.0-SNAPSHOT"

    val appDependencies = Seq(
      "net.debasishg" %% "redisclient" % "2.5",
      "io.netty" % "netty" % "3.5.8.Final"
    )

    val main = PlayProject(appName, appVersion, appDependencies, mainLang = SCALA).settings(
      // Add your own project settings here      
    )
}
