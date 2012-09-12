package actors

import akka.actor._
import play.api.libs.ws.{Response, WS}
import play.api.libs.concurrent.Akka
import play.api.Play.current
import akka.util.duration._
import play.api.Logger
import collection._
import immutable.HashSet
import play.api.libs.iteratee.{Enumerator, PushEnumerator}
import util.LinkUtility.findLinks
import akka.routing.{RoundRobinRouter, SmallestMailboxRouter}
import java.net.URL
import util.RobotsExclusion
import crawler.Global.redis

/**
 */
/// Request crawl for specific URL
case class CrawlRequest(val url : URL)
/// Results for individual crawl to url
case class CrawlResult(url: URL, status : Int, duration : Long, bytes : Long, links : Seq[URL])
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
case class CrawlStatistics(total: Int, success : Int, failed : Int, running : Long, bytes : Long)

case class ManagerStatisticsRequest()
case class ManagerStatistics(activeSites : Int, pendingSites : Int)

case class Stop()
/**
 * Class for crawling individual urls.
 *
 * Simple worker which retrieves given url, notifies statistics actor and returns results
 * @param statsActor
 */
class CrawlActor(statsActor : ActorRef) extends Actor {
  def receive = {
    case CrawlRequest(url) =>
      Logger.debug("Crawling url "+url+" with "+self)
      val targets = Seq(sender, statsActor)
      val start = System.currentTimeMillis();
      try {
        val r = WS.url(url.toString).get().await(10000).get
        val status = r.status
        val duration = System.currentTimeMillis() - start

        val res = CrawlResult(url, status, duration, r.body.size, findLinks(r.body, baseURL = Some(url)))
        Logger.debug("Finished crawling url %s in %dms with %s" format(url, duration, self))
        targets foreach (_ ! res)
      } catch {
        case e@_ =>
          val res = CrawlResult(url, 999, System.currentTimeMillis() - start, 0, Seq.empty)
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
  var stopping = false
  lazy val robots : RobotsExclusion = fetchRobots()

  lazy val crawler = context.actorOf(Props(new CrawlActor(CrawlManager.statistics))
    .withRouter(new SmallestMailboxRouter(concurrency))
    .withDispatcher("play.akka.actor.crawler-dispatcher"), name="crawler")


  def receive = {
    case LinksFound(urls) =>
      urls foreach ( addUrl(_) )

    case CrawlRequest(url) =>
      addUrl(url)

    case CrawlResult(url, status, duration, size, links) =>
      active = active.filterNot(_ equals url)
      redis.withClient {
        r =>
          r.zadd("visited:" + site, System.currentTimeMillis, url.toString())
          val (local, other) = links.partition(site equals _.getHost)

          CrawlManager.ref ! LinksFound(other.distinct)

          val newLinks = local.filterNot({
            u => r.zrank("visited:" + site, u.toString).isEmpty
          }).distinct

          pending = pending ++ newLinks
      }

      if (pending.nonEmpty) {
        val url = pending.head
        pending = pending.drop(1)
        Logger.debug("Site %s launching crawl from pending for %s with %s" format (site, url, self))
        crawler ! new CrawlRequest(url)
        active = active :+ url
      } else if (active.isEmpty) {
        if (stopping) {
          context.stop(self)
        } else
          CrawlManager.ref ! SiteCrawlFinished(site)
      }

    case Stop() =>
      stopping = true
      if (active.isEmpty && pending.isEmpty) {
        context.stop(self)
      }


    case msg @ _ => Logger.warn("Unknown message! "+msg)
  }

  def fetchRobots() : RobotsExclusion = {
    val url = "http://" + site + "/robots.txt"
    try {
      RobotsExclusion(WS.url(url).get().await(5000).get.body, "Crawler")
    } catch {
      case _ =>
        new RobotsExclusion(Seq.empty)
    }
  }

  def addUrl(url: URL) {
    redis.withClient {
      r =>
        if (r.zrank("visited:"+site, url.toString).isEmpty && !active.contains(url))
          if (robots.allow(url.getFile)) {
            if (active.size < 3 * concurrency) {
              // Keeping actors fed with messages so that they can process next message immediately
              Logger.debug("Site %s launching crawl for %s with %s" format(site, url, self))
              crawler ! new CrawlRequest(url)
              active = active :+ url
            } else {
              pending = pending + url
            }
          } else {
            Logger.debug("Skipped url %s due robots.txt" format (url))
          }
    }
  }
}



class CrawlStatisticsActor extends Actor {
  var total = 0
  var success = 0
  var failed = 0
  var totalSites = 0
  var pendingSites = 0
  var bytes : Long = 0
  val start = System.currentTimeMillis()
  var listeners = Seq.empty[PushEnumerator[String]]
  var lastStats = CrawlStatistics(0,0,0, System.currentTimeMillis(),0)

  Akka.system.scheduler.schedule(0 seconds, 1 seconds, self, "tick")


  def receive = {
    case "tick" =>
      CrawlManager.ref ! ManagerStatisticsRequest()

    case CrawlResult(url, status, duration, size, links) =>
      total += 1
      bytes += size
      status match {
        case 200 | 202 | 204 => success += 1
        case _ => failed += 1
      }

    case CrawlStatisticsRequest() =>
      Logger.debug("Received statistics request")
      listeners.foreach(_.push(statsHtml.toString))
      lastStats = CrawlStatistics(total, success, failed, System.currentTimeMillis(), bytes)

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

  def statsHtml = {
    val fromStart = (System.currentTimeMillis()-start)/1000.0
    val fromLast = (System.currentTimeMillis()-lastStats.running)/1000.0
    "<pre>" +
      ("total sites: active %d, pending %d\ncrawls: total %d, success %d, failure %d, duration %.2fs kB %.2f\n" format (totalSites, pendingSites, total, success, failed, fromStart, bytes/1024.0)) +
      ("delta crawls: total %.2f 1/s, %.2f kBs" format ((total-lastStats.total)/fromLast, (bytes-lastStats.bytes)/fromLast/1024.0)) +
    "</pre>"
  }

}

/**
 * Class for managing the crawl process
 *
 * @param concurrency How many sites can be crawled in parallel
 */
class CrawlManager(val concurrency : Int) extends Actor {
  val active : mutable.HashMap[String, ActorRef] = mutable.HashMap.empty

  def receive = {
    case LinksFound(urls) =>
      registerLink(urls)

    case FeedUrl(url) =>
      Logger.debug("Feeded system with url "+url)
      registerLink(Seq(new URL(url)))

    case SiteCrawlFinished(site) =>
      Logger.info("Finished site crawl for "+site)
      active.remove(site) map ( _ ! Stop())
      redis.withClient { r =>
        r.spop("observed_sites").foreach { newSite : String =>
          r.smembers("observed:"+newSite) map { urls =>
            launchSiteActor(newSite, urls.flatMap { url => url.map(new URL(_))}.toSeq)
          }
        }
      }

    case ManagerStatisticsRequest() =>
      redis.withClient { r =>
        sender ! ManagerStatistics(active.size, r.scard("observed_sites").getOrElse(0))
      }

    case msg @ _ => Logger.warn("Unknown message! "+msg)

  }

  def registerLink(urls : Seq[URL]) {
    val bySite = urls groupBy (_.getHost)

    bySite foreach { a =>
      val (site, siteUrls) = (a._1, a._2)

      active.get(site) match {
        case Some(actor) => actor ! LinksFound(siteUrls)
        case None =>
          if (active.size < concurrency) {
            Logger.debug("Creating new site actor for "+site)

            launchSiteActor(site, siteUrls)
          } else
            redis.withClient { r=>
              r.sadd("observed_sites", site)
              r.sadd("observed:"+site, siteUrls.head, siteUrls.drop(1) : _*)
            }
      }
    }
  }
  def launchSiteActor(site : String, urls : Seq[URL]) {
    val actor = context.actorOf(Props(new SiteActor(site)).withDispatcher("play.akka.actor.crawler-dispatcher"))
    active.put(site, actor)
    actor ! LinksFound(urls)

  }
}

object CrawlManager {
  lazy val system = Akka.system
  lazy val ref = system.actorOf(Props(new CrawlManager(150)).withDispatcher("play.akka.actor.manager-dispatcher"), name="manager")
  lazy val statistics = system.actorOf(Props[CrawlStatisticsActor].withDispatcher("play.akka.actor.statistics-dispatcher"), "statistics")

  Akka.system.scheduler.schedule(0 seconds, 5 seconds, statistics, CrawlStatisticsRequest())

  val listener = system.actorOf(Props(new Actor {
    def receive = {
      case d: DeadLetter => Logger.error(d.toString)
    }
  }))
  system.eventStream.subscribe(listener, classOf[DeadLetter])
}