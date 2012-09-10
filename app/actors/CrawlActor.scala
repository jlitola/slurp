package actors

import akka.actor._
import play.api.libs.ws.WS
import play.api.libs.concurrent.Akka
import play.api.Play.current
import akka.util.duration._
import play.api.Logger
import collection._
import play.api.libs.iteratee.{Enumerator, PushEnumerator}
import util.LinkUtility.findLinks
import akka.routing.SmallestMailboxRouter
import java.net.URL

/**
 */

case class CrawlRequest(val url : URL)
case class CrawlResult(url: URL, status : Int, duration : Long, links : Seq[URL])
case class FeedUrl(url : String)
case class SiteCrawlFinished(site : String)
case class Listen()
case class Quit(channel: PushEnumerator[String])
case class LinksFound(urls: Seq[URL])

/**
 * Class for crawling individual urls.
 *
 * Simple worker which retrieves given url, notifies statistics actor and returns results
 * @param statsActor
 */
class CrawlActor(statsActor : ActorRef) extends Actor {
  def receive = {
    case CrawlRequest(url) =>
      Logger.info("Crawling url "+url)
      val targets = Seq(sender, statsActor)
      val start = System.currentTimeMillis();
      WS.url(url.toString).get().map {r =>
        val status = r.status
        val duration = System.currentTimeMillis()-start

        val res = CrawlResult(url, status, duration, findLinks(r.body, baseURL=Some(url)))
        Logger.info("Finished crawling url %s in %dms" format (url, duration))
        targets foreach ( _ ! res )
      }
  }
}

/**
 * Class for managing crawl on one site / domain.
 *
 * Throttles queries so that target web sites do not crash under crawling. Independently crawls through the site,
 * and delegates links leading out of the site to CrawlManager.
 *
 * @param site Domain of the site
 * @param concurrency How many concurrent crawlers to use
 */
class SiteActor(val site : String, val concurrency : Int = 2) extends Actor {
  var pending = Seq.empty[URL]
  var active = Seq.empty[URL]
  var visited = Map.empty[URL, Long]

  def receive = {
    case r @ CrawlRequest(url) =>
      Logger.info("Site %s (%s) received crawl for %s" format (site, self, r.url))

      if (! visited.contains(url) )
        if (active.size < concurrency) {
          CrawlManager.crawler ! r
          active = active :+ r.url
        } else {
          pending = pending :+ r.url
        }

    case CrawlResult(url, status, duration, links) =>
      active = active.filterNot(_ equals url)
      val (local, other) = links.partition(_.getHost equals site)

      CrawlManager.ref ! LinksFound(other)

      val newLinks = local.filterNot { url =>
        visited.contains(url)
      }
      pending = (pending ++ newLinks) distinct


      if (pending.nonEmpty) {
        val url = pending.head
        pending = pending.drop(1)
        CrawlManager.crawler ! new CrawlRequest(url)
        active = active :+ url
      } else if (active.isEmpty) {
        CrawlManager.ref ! SiteCrawlFinished(site)
        context.stop(self)
      }
    case msg @ _ => Logger.info("Unknown message! "+msg)
  }

}

case class CrawlStatisticsRequest()
case class CrawlStatistics(total: Int, success : Int, failed : Int, running : Long)


class CrawlStatisticsActor extends Actor {
  var total = 0
  var success = 0
  var failed = 0
  val start = System.currentTimeMillis()
  var listeners = Seq.empty[PushEnumerator[String]]

  def receive = {
    case CrawlResult(url, status, duration, links) =>
      Logger.info("Registering statistics")
      total += 1
      status match {
        case 200 => success += 1
        case _ => failed += 1
      }

    case CrawlStatisticsRequest() =>
      Logger.info("Received statistics request")
      listeners.foreach(_.push("total %d, success %d, failure %d, duration %dms" format (total, success, failed, System.currentTimeMillis()-start)))

    case Listen() =>
      lazy val channel: PushEnumerator[String] = Enumerator.imperative[String](
        onComplete = self ! Quit(channel)
      )
      listeners = listeners :+ channel
      Logger.info("Added listener "+channel+" has "+listeners.size+" listeners")
      sender ! channel

    case Quit(channel) =>
      listeners = listeners.filterNot(_ == channel)
      Logger.info("Removed listener "+channel+" has "+listeners.size+" listeners")
  }


}

/**
 * Class for managing the crawl process
 *
 * @param concurrency How many sites can be crawled in parallel
 */
class CrawlManager(val concurrency : Int) extends Actor {
  val active : mutable.HashMap[String, ActorRef] = mutable.HashMap.empty
  val pending : mutable.HashMap[String, Seq[URL]] = mutable.HashMap.empty

  def receive = {
    case LinksFound(urls) =>
      urls foreach (registerLink _)

    case FeedUrl(url) =>
      Logger.info("Feeded system with url "+url)
      registerLink(new URL(url))

    case SiteCrawlFinished(site) =>
      active -= site
      pending.headOption foreach { k =>
        val (newSite, urls) = (k._1, k._2)

        pending -= newSite
        val actor = context.actorOf(Props(new SiteActor(site)))
        active.put(newSite, actor)
        urls foreach (actor ! CrawlRequest(_))
      }


    case msg @ _ => Logger.info("Unknown message! "+msg)

  }

  def registerLink(url : URL) {
    val site = url.getHost()

    active.get(site) match {
      case Some(actor) => actor ! CrawlRequest(url)
      case None =>
        if (active.size < concurrency) {
          Logger.info("Creating new site actor for "+site)

          val actor = context.actorOf(Props(new SiteActor(site)))
          active.put(site, actor)
          actor ! CrawlRequest(url)

        } else
          pending.put(site, pending.get(site).getOrElse(Seq.empty[URL]) :+ url)
    }
  }
}

object CrawlManager {
  lazy val system = Akka.system
  lazy val ref = system.actorOf(Props(new CrawlManager(100)).withDispatcher("play.akka.actor.manager-dispatcher"), name="manager")
  lazy val statistics = system.actorOf(Props[CrawlStatisticsActor].withDispatcher("play.akka.actor.statistics-dispatcher"), "statistics")
  lazy val crawler = system.actorOf(Props(new CrawlActor(statistics)).withDeploy(new Deploy("/crawlers")))

  Akka.system.scheduler.schedule(0 seconds, 10 seconds, statistics, CrawlStatisticsRequest())
}