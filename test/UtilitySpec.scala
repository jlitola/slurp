import org.specs2.mutable._


import play.api.test._
import play.api.test.Helpers._
import org.specs2.specification.Scope
import util.{RobotsExclusion, LinkUtility}
import java.net.URL

trait robots extends Scope {
  val rule = """User-agent: *
Disallow: /private

User-agent: Slurp
Disallow: /nocrawler"""
  val robot = Robots(rule, "Slurp")
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
    val baseUrl = new URL("http://foo.com")

    "handle relative urls" in {
      val links = LinkUtility.findLinks("""<a href="/foo">Link</a>""", baseURL = Some(baseUrl))
      links must have size(1)
      links.head.toString mustEqual """http://foo.com/foo"""
    }
  }

  "RobotsExclusion" should {
    "be able to parse robots.txt" in new robots {
      robot must haveClass[Robots]
    }
    "allow access based on rules" in new robots {
      robot.allow("/anything") must beTrue
      robot.allow("/nocrawler/") must beFalse
      robot.allow("/private/") must beFalse
    }
  }
}
