package util

import java.net.{MalformedURLException, URL}
import play.api.libs.ws.{ResponseHeaders, WS, Response}
import play.api.libs.iteratee.{Iteratee, Enumeratee}
import play.api.Logger

class UnsupportedContentType (val contentType : String) extends Exception


class Link(b : String, p : String) {
  val ProtoPattern = """^(\w+):.*""".r
  val FromRoot = """^(/.*)""".r

  val ParentReplace = """^/../|/[^/]+/\.\.(/|\Z)""".r
  val DoubleSlash = """//""".r
  val FilePart = "/[^/]+$".r

  val base = b
  val path = if (p==null || "".equals(p)) "/" else p

  def proto = {
    base match {
      case ProtoPattern(proto) =>
        proto
    }
  }

  def childLink( url : String) = {
    url match {
      case Link.HTTPUrlPattern(b, p) =>
        new Link(b,p)
      case Link.UnknownUrlPattern(b, p) =>
        new Link(b,p)
      case FromRoot(path) =>
        new Link(base, path)
      case p : String =>
        val np = FilePart.replaceFirstIn(path, "")
        new Link(base, trimPath(np+"/"+p))
    }
  }
  def trimPath(path : String) = {
    var result = DoubleSlash.replaceAllIn(path, "/")
    path
    var source = path
    do {
      source = result
      result = ParentReplace.replaceAllIn(source, "/")
    } while(! source.equals(result) )
    result
  }
  override def toString = {
    base+path
  }
}

object Link {
  val UnknownUrlPattern = """^(\w+:)(.*+)""".r
  val HTTPUrlPattern =
    """((?:\w+:)?//(?:.*?:.*?@)?[^/]+(?::\d+)?)(/.*?)?(?:#.*)?""".r
  def apply(url : String) = {
    url match {
      case HTTPUrlPattern(b, p) =>
        new Link(b, p)
      case UnknownUrlPattern(b, p) =>
        new Link(b, p)
    }
  }
}

object LinkUtility {
  val LinkPattern = """(?s)(<a[^>]*>)""".r
  val NoFollow = """.*\brel=['"]?nofollow['"]?.*""".r
  val HRef = """.*\bhref=['"]?([^'" ]+).*""".r
  val Anchor = """#.*""".r

  def findLinks(document: String, baseURL: Option[Link] = None): Seq[Link] = {
    val res = LinkPattern findAllIn (document) flatMap {
      case NoFollow() => None
      case HRef(url) =>
        val trimmedUrl = Anchor.replaceFirstIn(url, "")
        try {
          val computedUrl = baseURL match {
              case Some(base) =>
                base.childLink(trimmedUrl)
              case None => Link(trimmedUrl)
            }
          val proto = computedUrl.proto
          if(proto.equals("http") || proto.equals("https"))
            Some(computedUrl)
          else
            None
        } catch {
          case e: MatchError => None
        }
      case _ => None
    }
    res.toList.toSeq
  }

  case class ResponseDetails(response : ResponseHeaders, size : Int, links : Seq[Link])

  def byteStreamToLinksIteratee(r : ResponseHeaders, url : Link ) : Iteratee[Array[Byte], LinkUtility.ResponseDetails]= {
    if(r.status==301 || r.status==302) {
      val links = r.headers.get("Location").collect {
        case Seq(u : String) => Seq(url.childLink(u))
      }.getOrElse(Seq.empty[Link])
      return Iteratee.fold[Array[Byte], ResponseDetails](ResponseDetails(r, 0, links)) { (details, bytes) => details}
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

}
