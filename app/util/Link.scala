package util

import java.net.{MalformedURLException, URL}
import play.api.libs.ws.{ResponseHeaders, WS, Response}
import play.api.libs.iteratee.{Iteratee, Enumeratee}
import play.api.Logger

class UnsupportedContentType (val contentType : String) extends Exception


object LinkUtility {
  val LinkPattern = """(?s)(<a[^>]*>)""".r
  val NoFollow = """.*\brel=['"]?nofollow['"]?.*""".r
  val HRef = """.*\bhref=['"]?([^'" ]+).*""".r
  val Anchor = """#.*""".r

  def getPath(url : URL ) = {
    val path = url.getPath
    if ("" equals path)
      "/"
    else path
  }

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
    res.toList.toSeq
  }

  case class ResponseDetails(response : ResponseHeaders, size : Int, links : Seq[URL])

  def byteStreamToLinksIteratee(r : ResponseHeaders, url : URL ) : Iteratee[Array[Byte], LinkUtility.ResponseDetails]= {
    if(r.status==301 || r.status==302) {
      val links = r.headers.get("Location").collect {
        case Seq(u : String) => Seq(new URL(url, u))
      }.getOrElse(Seq.empty[URL])
      return Iteratee.fold[Array[Byte], ResponseDetails](ResponseDetails(r, 0, links)) { (details, bytes) => details}
    }
    val t = r.headers.get("Content-Type").getOrElse(Seq("text/html"))(0)
    if (!t.startsWith("text/html")) {
      Logger.debug("Ignoring "+url+" as the content type is "+t)
      throw new UnsupportedContentType(t)
    }
    val byteToLine = new Object() {
      var remaining = ""
      var size = 0
      def apply(bytes: Array[Byte]) : Seq[String] = {
        size+=bytes.size
        val lines = (remaining + new String(bytes)).split("\n")
        remaining = lines.last
        lines.dropRight(1)
      }
    }
    Enumeratee.map[Array[Byte]] { byteToLine(_) } &>>
      Iteratee.fold[Seq[String], ResponseDetails](ResponseDetails(r, 0, Seq.empty)) {
        (details, lines) =>
          details.copy(
            links = details.links ++ (lines flatMap (findLinks(_, Some(url)))),
            size = byteToLine.size
          )
      }
  }

  def baseUrl(url : URL) : String = {
    val port = url.getPort
    val s = new StringBuilder(url.getProtocol)
    s.append("://")
    s.append(url.getHost)
    if (port != -1 && port!=url.getDefaultPort) {
      s.append(":")
      s.append(url.getPort.toString)
    }
    s.toString
  }

}
