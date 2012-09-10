import org.specs2.mutable._


import play.api.test._
import play.api.test.Helpers._
import util.LinkUtility
import java.net.URL

class UtilitySpec extends Specification  {
  "LinkUtility.findLinks" should {
    "find simple link" in {
      LinkUtility.findLinks("""<a href="http://foo.com">Link</a>""") must have size(1)
    }
    "ignore nofollow link" in {
      LinkUtility.findLinks("""<a href="http://foo.com" rel="nofollow">Link</a>""") must have size(0)
    }
    "gracefully ignore if there are no links" in {
      LinkUtility.findLinks("""Lorem ipsum...</a>""") must have size(0)
    }
    "match multiple links" in {
      LinkUtility.findLinks("""<a href="http://foo.com">Link</a>\n<a href="http://foo.com" rel="nofollow">Link</a><a href="http://bar.com">Link</a>""") must have size(2)
    }
    "notice malformed urls" in {
      LinkUtility.findLinks("""<a href="this is broken url">Link</a>""") must have size(0)
    }
    val baseUrl = new URL("http://foo.com")

    "handle relative urls" in {
      val links = LinkUtility.findLinks("""<a href="/foo">Link</a>""", baseURL = Some(baseUrl))
      links must have size(1)
      links.head.toString mustEqual """http://foo.com/foo"""
    }
  }
}
