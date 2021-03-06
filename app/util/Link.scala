package util

import java.net.{MalformedURLException, URL}
import play.api.libs.ws.{ResponseHeaders, WS, Response}
import play.api.libs.iteratee.{Iteratee, Enumeratee}
import play.api.Logger

class UnsupportedContentType (val contentType : String) extends Exception
class HttpError (val status : Int) extends Exception


object LinkUtility {
  val LinkPattern = """(?s)(<a.*?>)""".r
  val NoFollow = """.*\brel=['"]?nofollow['"]?.*""".r
  val HRef = """.*\bhref\s*=\s*(?:"([^"\s]+)"|'([^'\s]+)'|([^"'\s]+)).*""".r
  val ValidURL = """[\w\d:#@%/;$()~_?\+-=\\\.&]+""".r
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
      case HRef(url1, url2, url3) =>
        val url =
          if (url1!=null) url1
            else if (url2!=null) url2
            else url3
        url match {
          case ValidURL() =>
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
      case _ => None
    }
    res.toList.toSeq
  }

  case class ResponseDetails(response : ResponseHeaders, size : Int, links : Seq[URL])

  def byteStreamToLinksIteratee(r : ResponseHeaders, url : URL ) : Iteratee[Array[Byte], LinkUtility.ResponseDetails]= {
    r.status match {
      case 301 | 302 =>
        val links = r.headers.get("Location").collect {
          case Seq(u : String) => Seq(new URL(url, u))
        }.getOrElse(Seq.empty[URL])
        return Iteratee.fold[Array[Byte], ResponseDetails](ResponseDetails(r, 0, links)) { (details, bytes) => details}
      case status : Int if status >= 400 =>
        throw new HttpError(status)
      case _ =>
    }
    val t = r.headers.get("Content-Type").getOrElse(Seq("text/html"))(0)
    if (!t.startsWith("text/html")) {
      Logger.debug("Ignoring "+url+" as the content type is "+t)
      throw new UnsupportedContentType(t)
    }
    val byteToLine = new Object() {
      var remaining = new StringBuilder
      var size = 0
      def apply(bytes: Array[Byte]) : String = {
        size+=bytes.size
        remaining.append(new String(bytes))
        val i = remaining.lastIndexOf("\n")
        if(i != -1) {
          val block = remaining.substring(0, i)
          remaining.delete(0,i)
          block
        } else
          ""
      }
    }
    Enumeratee.map[Array[Byte]] { byteToLine(_) } &>>
      Iteratee.fold[String, ResponseDetails](ResponseDetails(r, 0, Seq.empty)) {
        (details, lines) =>
          details.copy(
            links = details.links ++ findLinks(lines, Some(url)),
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
