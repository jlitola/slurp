package actors

import akka.actor._
import play.api.libs.ws.WS
import play.api.libs.concurrent.Akka
import play.api.Play.current
import akka.util.duration._
import play.api.Logger
import collection._
import immutable.HashSet
import play.api.libs.iteratee.{Enumerator, PushEnumerator}
import util.LinkUtility.findLinks
import akka.routing.SmallestMailboxRouter
import java.net.URL

/**
 */
/// Request crawl for specific URL
case class CrawlRequest(val url : URL)
/// Results for individual crawl to url
case class CrawlResult(url: URL, status : Int, duration : Long, links : Seq[URL])
/// Feed new url to system
case class FeedUrl(url : String)
/// Notify that crawling of one site has finished
case class SiteCrawlFinished(site : String)
/// Listen for the statistics
case class Listen()
/// Quit listening on channel
case class Quit(channel: PushEnumerator[String])
/// Notify about the links found
case class LinksFound(urls: Seq[URL])
/// Request for Crawl statistics
case class CrawlStatisticsRequest()
/// Current statistics for the crawl
case class CrawlStatistics(total: Int, success : Int, failed : Int, running : Long)

case class ManagerStatisticsRequest()
case class ManagerStatistics(activeSites : Int, pendingSites : Int)
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
      try {
      WS.url(url.toString).get().map {r =>
        val status = r.status
        val duration = System.currentTimeMillis()-start

        val res = CrawlResult(url, status, duration, findLinks(r.body, baseURL=Some(url)))
        Logger.info("Finished crawling url %s in %dms" format (url, duration))
        targets foreach ( _ ! res )
      }
      } catch {
        case e @ _ =>
          val res = CrawlResult(url, 999, System.currentTimeMillis()-start, Seq.empty)
          targets foreach (_ ! res)
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
  var pending = HashSet.empty[URL]
  var active = Seq.empty[URL]
  var visited = Map.empty[URL, Long]

  def receive = {
    case LinksFound(urls) =>
      urls foreach ( addUrl(_) )

    case CrawlRequest(url) =>
      addUrl(url)

    case CrawlResult(url, status, duration, links) =>
      active = active.filterNot(_ equals url)
      visited = visited + (url -> System.currentTimeMillis())
      val (local, other) = links.partition(_.getHost equals site)

      CrawlManager.ref ! LinksFound(other)

      val newLinks = local.filterNot( visited.contains(_) )

      pending = pending ++ newLinks

      if (pending.nonEmpty) {
        val url = pending.head
        pending = pending.drop(1)
        Logger.info("Site %s launching crawl from pending for %s" format (site, url))
        CrawlManager.crawler ! new CrawlRequest(url)
        active = active :+ url
      } else if (active.isEmpty) {
        CrawlManager.ref ! SiteCrawlFinished(site)
        context.stop(self)
      }
    case msg @ _ => Logger.info("Unknown message! "+msg)
  }

  def addUrl(url : URL) {
    if (! visited.contains(url) && ! active.contains(url) )
      if (active.size < concurrency) {
        Logger.info("Site %s launching crawl for %s" format (site, url))
        CrawlManager.crawler ! CrawlRequest(url)
        active = active :+ url
      } else {
        pending = pending + url
      }
  }
}



class CrawlStatisticsActor extends Actor {
  var total = 0
  var success = 0
  var failed = 0
  var totalSites = 0
  var pendingSites = 0
  val start = System.currentTimeMillis()
  var listeners = Seq.empty[PushEnumerator[String]]

  Akka.system.scheduler.schedule(0 seconds, 1 seconds, self, "tick")


  def receive = {
    case "tick" =>
      CrawlManager.ref ! ManagerStatisticsRequest()
    case CrawlResult(url, status, duration, links) =>
      total += 1
      status match {
        case 200 => success += 1
        case _ => failed += 1
      }

    case CrawlStatisticsRequest() =>
      Logger.info("Received statistics request")
      listeners.foreach(_.push("sites: %d active, %d pending\ncrawls: total %d, success %d, failure %d, duration %dms"
        format (totalSites, pendingSites, total, success, failed, System.currentTimeMillis()-start)))

    case ManagerStatistics(t, p) => totalSites = t; pendingSites = p

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
  val pending : mutable.HashMap[String, HashSet[URL]] = mutable.HashMap.empty

  def receive = {
    case LinksFound(urls) =>
      urls foreach (registerLink _)

    case FeedUrl(url) =>
      Logger.info("Feeded system with url "+url)
      registerLink(new URL(url))

    case SiteCrawlFinished(site) =>
      active -= site
      Logger.info("Finished site crawl for "+site+ "pending sites " + pending.size)
      pending.headOption foreach { k =>
        val (newSite, urls) = (k._1, k._2)

        pending -= newSite
        Logger.info("Initiating site crawl for "+site)
        val actor = context.actorOf(Props(new SiteActor(site)))
        active.put(newSite, actor)
        urls foreach (actor ! CrawlRequest(_))
      }

    case ManagerStatisticsRequest() =>
      sender ! ManagerStatistics(active.size, pending.size)

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
          pending.put(site, pending.get(site).getOrElse(HashSet.empty[URL]) + url)
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