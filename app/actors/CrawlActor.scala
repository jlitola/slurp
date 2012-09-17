package actors

import akka.actor._
import play.api.libs.ws._
import play.api.libs.concurrent.Akka
import play.api.Play.current
import akka.util.duration._
import play.api.{Play, Logger}
import collection._
import immutable.HashSet
import play.api.libs.iteratee._
import akka.routing.{RoundRobinRouter, SmallestMailboxRouter}
import java.net.URL
import util.{UnsupportedContentType, LinkUtility, RobotsExclusion}
import crawler.Global.redis
import com.redis.RedisClient
import akka.dispatch.ExecutionContext

/**
 */
/// Request crawl for specific URL
case class CrawlRequest(val url : URL)
/// Results for individual crawl to url
case class CrawlResult(url: URL, status : CrawlStatus, duration : Long, bytes : Long, links : Seq[URL])
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
case class CrawlStatistics(total: Int, success : Int, failed : Int, ignored : Int, redirect : Int, running : Long, bytes : Long)

case class ManagerStatisticsRequest()
case class ManagerStatistics(activeSites : Int, pendingSites : Int)

case class Stop()

trait CrawlStatus

case class CrawlHttpStatus(status : Int) extends CrawlStatus
case class SkippedContentType(contentType : String) extends CrawlStatus
case class CrawlException(exception : Throwable) extends CrawlStatus

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
      val start = System.nanoTime();
      try {
        WS.url(url.toString).get{ response =>
          LinkUtility.byteStreamToLinksIteratee(response, url)
        }.map(_.run).map(_.map { details =>
          sendResults(targets, CrawlResult(url, CrawlHttpStatus(details.response.status), System.nanoTime-start, details.size, details.links))
        }).recover {
          case e : UnsupportedContentType => sendResults(targets, CrawlResult(url, SkippedContentType(e.contentType), System.nanoTime() - start, 0, Seq.empty))
          case e @ _ => sendResults(targets, CrawlResult(url, CrawlException(e), System.nanoTime() - start, 0, Seq.empty))
        }
      } catch {
        case e @ _ => sendResults(targets, CrawlResult(url, CrawlException(e), System.nanoTime() - start, 0, Seq.empty))
      }
  }

  def sendResults(targets : Seq[ActorRef], result : CrawlResult) = {
    Logger.debug("Finished crawling url %s with status (%s) in %dms with %s" format(result.url, result.status, result.duration/1000000, self))
    targets foreach (_ ! result)
  }

  override def preRestart(reason : Throwable , message : Option[Any]) {
    Logger.error("CrawlActor got restarted due %s with message %s" format(reason, message))
    super.preRestart(reason,message)
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
  var pending = Set.empty[String]
  var active = Seq.empty[String]
  var visited = Map.empty[String, Long]
  var stopping = false
  lazy val robots : RobotsExclusion = fetchRobots()

  def receive = {
    case LinksFound(urls) =>
      redis.withClient {
        implicit r =>
          urls foreach ( addUrl(_) )
          if(active.isEmpty && !stopping) notifyCrawlFinished
      }

    case CrawlRequest(url) =>
      redis.withClient {
        implicit r =>
          addUrl(url)
          if(active.isEmpty && !stopping) notifyCrawlFinished
      }

    case CrawlResult(url, status, duration, size, links) =>
      active = active.filterNot(_ equals url.getPath)
      redis.withClient {
        implicit r =>
          val time = System.currentTimeMillis
          r.zadd("visited:" + site, time, url.toString())
          visited = visited + (url.getPath -> time)

          val (local, other) = links.partition(site equals LinkUtility.baseUrl(_))

          CrawlManager.ref ! LinksFound(other)

          pending = pending ++ local.map(_.getPath).filterNot( visited.contains( _ ) )

          pending = pending.dropWhile(!shouldCrawl(_))

          if(pending.nonEmpty) {
            launchCrawl(pending.head)
            pending = pending.tail
          }

          if (pending.isEmpty && active.isEmpty) {
            if (stopping) {
              Logger.info("Stopping site "+site+" context as we ran out of work and are stopping")
              context.stop(self)
            } else
              notifyCrawlFinished
          }
      }

    case Stop() =>
      stopping = true
      if (active.isEmpty && pending.isEmpty) {
        Logger.info("Stopping site "+site+" context as there is no activity")
        context.stop(self)
      }


    case msg @ _ => Logger.warn("Unknown message! "+msg)
  }

  override def preStart() {
    redis.withClient { r=>
      r.zrangeWithScore("visited:"+site, 0, -1) map { l =>
        visited = visited ++ l.map { v =>
          val (url, score) = (v._1, v._2)
          (new URL(url).getPath -> score.asInstanceOf[Long])
        }
      }
    }
  }

  override def preRestart(reason : Throwable , message : Option[Any]) {
    Logger.error("Actor for site %s got restarted due %s with message %s" format(site, reason, message))
    super.preRestart(reason,message)
  }

  override def postStop() {
    visited = Map.empty
    active = Seq.empty
    pending = Set.empty
  }

  def notifyCrawlFinished {
    Logger.info("Notifying manager that site crawl for %s is finished" format (site))
    CrawlManager.ref ! SiteCrawlFinished(site)
  }

  def fetchRobots() : RobotsExclusion = {
    val url = site + "/robots.txt"
    try {
      RobotsExclusion(WS.url(url).get().await(5000).get.body, "Slurp")
    } catch {
      case _ =>
        new RobotsExclusion(Seq.empty)
    }
  }

  def shouldCrawl(path : String)(implicit r : RedisClient) : Boolean = {
    !active.contains(path) && robots.allow(path) && ! visited.contains(path)
  }

  def launchCrawl(path : String) {
    Logger.debug("Site %s launching crawl for %s with %s" format(site, path, self))
    val p = if (path.startsWith("/")) path else "/"+path
    val url = new URL(site+p)
    CrawlManager.crawler ! new CrawlRequest(url)
    active = active :+ p
  }

  /**
   * Add url for processing
   * @return Whether url was accepted
   */
  def addUrl(url: URL)(implicit r: RedisClient) {
    val path = url.getPath
    if (shouldCrawl(path)) {
      if (active.size < concurrency) {
        launchCrawl(path)
      } else {
        pending = pending + path
      }
    }
  }
}



class CrawlStatisticsActor extends Actor {
  var total = 0
  var success = 0
  var failed = 0
  var ignored = 0
  var redirect = 0
  var totalSites = 0
  var pendingSites = 0
  var bytes : Long = 0
  val start = System.nanoTime()
  var listeners = Seq.empty[PushEnumerator[String]]
  var lastStats = CrawlStatistics(0,0,0,0,0, System.nanoTime(),0)

  Akka.system.scheduler.schedule(0 seconds, 1 seconds, self, "tick")

  def receive = {
    case "tick" =>
      CrawlManager.ref ! ManagerStatisticsRequest()

    case CrawlResult(url, status, duration, size, links) =>
      total += 1
      bytes += size
      status match {
        case CrawlHttpStatus(200 | 202 | 204) => success += 1
        case CrawlHttpStatus(301 | 302) =>
          Logger.debug("url "+url+" is redirected to "+links)
          redirect += 1
        case CrawlHttpStatus(status) if status > 400 =>
          failed += 1
          Logger.error("HTTP error "+status+" at "+url)
        case SkippedContentType(contentType) => ignored += 1
        case _ =>
          failed += 1
          Logger.error("Status needing handling "+status)
      }

    case CrawlStatisticsRequest() =>
      Logger.debug("Received statistics request")
      listeners.foreach(_.push(statsHtml.toString))
      lastStats = CrawlStatistics(total, success, failed, ignored, redirect, System.nanoTime(), bytes)

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
    val fromStart = (System.nanoTime()-start)/1000000000.0
    val fromLast = (System.nanoTime()-lastStats.running)/1000000000.0
    "<pre>" +
      ("total sites: active %d, pending %d\ncrawls: total %d, success %d, failure %d, ignored %d, redirect %d, duration %.2fs kB %.2f\n" format (totalSites, pendingSites, total, success, failed, ignored, redirect, fromStart, bytes/1024.0)) +
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
  lazy val redisClient : RedisClient = redis.pool.borrowObject().asInstanceOf[RedisClient]

  override def postStop() {
    redis.pool.returnObject(redisClient)
  }

  def receive = {
    case LinksFound(urls) =>
      registerLink(urls)

    case FeedUrl(url) =>
      Logger.debug("Feeded system with url "+url)
      registerLink(Seq(new URL(url)))

    case SiteCrawlFinished(site) =>
      Logger.info("Finished site crawl for "+site)
      active.remove(site) map { actor =>
        Logger.debug("Sending stop to site "+site)
        actor ! Stop()

        redisClient.spop("observed_sites").foreach { newSite : String =>
          redisClient.smembers("observed:"+newSite) map { urls =>
            redisClient.srem("observed:"+newSite, urls.head, urls.tail)
            launchSiteActor(newSite, urls.flatten.flatMap { url =>
              try {
                Some(new URL(url))
              } catch {
                case _ => None
              }
            }.toSeq)
          }
        }
      }

    case ManagerStatisticsRequest() =>
      sender ! ManagerStatistics(active.size, redisClient.scard("observed_sites").getOrElse(0))

    case msg @ _ => Logger.warn("Unknown message! "+msg)

  }

  def registerLink(urls : Seq[URL]) {
    urls groupBy (LinkUtility.baseUrl(_)) foreach { a =>
      val (site, siteUrls) = (a._1, a._2)

      active.get(site) match {
        case Some(actor) => actor ! LinksFound(siteUrls)
        case None =>
          if (active.size < concurrency) {
            launchSiteActor(site, siteUrls)
          } else {
            redisClient.pipeline { r=>
              r.sadd("observed_sites", site)
              r.sadd("observed:"+site, siteUrls.head, siteUrls.tail : _*)
            }
          }
      }
    }
  }
  def launchSiteActor(site : String, urls : Seq[URL]) {
    Logger.info("Creating new site actor for "+site)
    val actor = context.actorOf(Props(new SiteActor(site)).withDispatcher("play.akka.actor.site-dispatcher"))
    active.put(site, actor)
    actor ! LinksFound(urls)

  }
}

object CrawlManager {
  lazy val system = Akka.system
  lazy val ref = system.actorOf(Props(new CrawlManager(Play.configuration.getInt("slurp.parallel.sites").getOrElse(100))).withDispatcher("play.akka.actor.manager-dispatcher"), name="manager")
  lazy val statistics = system.actorOf(Props[CrawlStatisticsActor].withDispatcher("play.akka.actor.statistics-dispatcher"), "statistics")
  lazy val crawler = system.actorOf(Props(new CrawlActor(statistics)).withRouter(new SmallestMailboxRouter(2*Runtime.getRuntime.availableProcessors)).withDispatcher("play.akka.actor.crawler-dispatcher"), "crawler")

  Akka.system.scheduler.schedule(0 seconds, 5 seconds, statistics, CrawlStatisticsRequest())

  val listener = system.actorOf(Props(new Actor {
    def receive = {
      case d: DeadLetter => Logger.error(d.toString)
    }
  }))
  system.eventStream.subscribe(listener, classOf[DeadLetter])
}
