package util

import java.net.{MalformedURLException, URL}

object LinkUtility {
  val LinkPattern = """(?s)(<a[^>]*>)""".r
  val NoFollow = """.*\brel=['"]?nofollow['"]?.*""".r
  val HRef = """(?s).*\bhref=['"]?([^'"]+).*""".r
  val Anchor = """#.*""".r

  def findLinks(document: String, baseURL: Option[URL] = None): Seq[URL] = {
    val res = LinkPattern findAllIn (document) flatMap {
      case NoFollow() => None
      case HRef(url) =>
        val trimmedUrl = Anchor.replaceFirstIn(url, "")
        try {
          val computedUrl = baseURL match {
              case Some(base) => new URL(base, trimmedUrl)
              case None => new URL(trimmedUrl)
            }
          val proto = computedUrl.getProtocol
          if(proto.equals("http") || proto.equals("https"))
            Some(computedUrl)
          else
            None
        } catch {
          case e: MalformedURLException => None
        }
      case _ => None
    }
    res.toSeq.distinct
  }
}
