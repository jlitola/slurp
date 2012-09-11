package util

import play.api.Logger

class RobotsExclusion(val forbidden : Seq[String]) {
  def allow(path : String) : Boolean = {
    val p = if (path.isEmpty()) "/" else path
    ! forbidden.exists(p.startsWith(_))
  }
}

object RobotsExclusion {
  val CommentPattern = """#.*""".r
  val Disallow = """Disallow:\s*(.*?)\s*""".r
  val UserAgent = """User\-agent:\s*(.*?)\s*""".r

  def apply(text : String, agent : String) : RobotsExclusion = {
    var capture = true
    new RobotsExclusion(Seq.empty[String] ++ text.lines flatMap { l =>
      val line = CommentPattern.replaceFirstIn(l, "")
      line match {
        case UserAgent(ua : String) =>
          capture = (ua.equals("*") || ua.contains(agent))
          None
        case Disallow(path) =>
          if (capture)
            if(path.equals("")) None else Some(path)
          else
            None
        case _ => None
      }
    })
  }
}
