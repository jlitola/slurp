import org.specs2.mutable._


import play.api.test._
import play.api.test.Helpers._
import org.specs2.specification.Scope
import util.{Link, RobotsExclusion, LinkUtility}
import java.net.URL

trait robots extends Scope {
  val rule = """User-agent: *
Disallow: /private

User-agent: Slurp
Disallow: /nocrawler"""
  val robot = RobotsExclusion(rule, "Slurp")
}

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
    val baseUrl = Link("http://foo.com")

    "handle relative urls" in {
      val links = LinkUtility.findLinks("""<a href="/foo">Link</a>""", baseURL = Some(baseUrl))
      links must have size(1)
      links.head.toString mustEqual """http://foo.com/foo"""
    }
  }

  "RobotsExclusion" should {
    "be able to parse robots.txt" in new robots {
      robot must haveClass[RobotsExclusion]
    }
    "allow access based on rules" in new robots {
      robot.allow("/anything") must beTrue
      robot.allow("/nocrawler/") must beFalse
      robot.allow("/private/") must beFalse
    }
  }
}

class LinkSpec extends Specification {
  "Link" should {
    "parse bare base url" in {
      val u = Link("http://foo.com")
      u.base mustEqual "http://foo.com"
      u.path mustEqual "/"
    }
    "parse url with path" in {
      val u = Link("https://foo.com/some/path")
      u.base mustEqual "https://foo.com"
      u.path mustEqual "/some/path"
    }
    "ignore anchor" in {
      val u = Link("https://foo.com/some/path#anchor")
      u.base mustEqual "https://foo.com"
      u.path mustEqual "/some/path"
    }
    "handle absolute child link" in {
      val u = Link("https://foo.com/some/path")
      val v = u.childLink("http://bar.com/")
      v.base mustEqual "http://bar.com"
      v.path mustEqual "/"
    }
    "handle absolute child link to javascript" in {
      val u = Link("https://foo.com/some/path")
      val v = u.childLink("javascript:alert('foo')")
      v.base mustEqual "javascript:"
      v.path mustEqual "alert('foo')"
    }
    "handle relative child link starting from /" in {
      val u = Link("https://foo.com/some/path")
      val v = u.childLink("/another/path")
      v.base mustEqual "https://foo.com"
      v.path mustEqual "/another/path"
    }
    "handle relative child link ending in /" in {
      val u = Link("https://foo.com/some/path/")
      val v = u.childLink("another")
      v.base mustEqual "https://foo.com"
      v.path mustEqual "/some/path/another"
    }
    "handle relative child link ending with non /" in {
      val u = Link("https://foo.com/some/path")
      val v = u.childLink("another")
      v.base mustEqual "https://foo.com"
      v.path mustEqual "/some/another"
    }
    "handle .." in {
      val u = Link("https://foo.com/some/path/")
      val v = u.childLink("../another")
      v.base mustEqual "https://foo.com"
      v.path mustEqual "/some/another"

      u.childLink("../../another").path mustEqual "/another"
      u.childLink("../../../another").path mustEqual "/another"
      u.childLink("../foo/../bar").path mustEqual "/some/bar"
    }
  }
}
