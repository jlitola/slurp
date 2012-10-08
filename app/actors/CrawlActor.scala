package actors

import akka.actor._
import play.api.libs.ws._
import play.api.libs.concurrent.Akka
import play.api.Play.current
import akka.util.duration._
import play.api.{Play, Logger}
import collection._
import akka.pattern.ask
import immutable.{HashMap, HashSet}
import play.api.libs.iteratee._
import akka.routing.{RoundRobinRouter, SmallestMailboxRouter}
import java.net.URL
import util.{HttpError, UnsupportedContentType, LinkUtility, RobotsExclusion}
import LinkUtility.getPath
import crawler.Global.redis
import com.redis.RedisClient
import akka.dispatch.{Await, Future, ExecutionContext}
import java.io._
import io.Source
import java.util.UUID
import akka.util.Timeout
import scala.Some
import akka.actor.DeadLetter

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
case class CrawlStatistics(total: Int, success : Int, failed : Int, ignored : Int, redirect : Int, timeout : Int, duration : Long, running : Long, bytes : Long)

case class VisitedSite(site : String, paths : Seq[String])
case class RequestSite()
case class ObservedSite(site : String, paths : Seq[String])
case class NoObservedSites()

case class ManagerStatisticsRequest()
case class ManagerStatistics(activeSites : Int, pendingSites : Int)

case class Stop()

trait CrawlStatus

case class CrawlHttpStatus(status : Int) extends CrawlStatus
case class SkippedContentType(contentType : String) extends CrawlStatus
case class CrawlTimeout() extends CrawlStatus
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
      val start = System.nanoTime
      try {
        WS.url(url.toString).get{ response =>
          LinkUtility.byteStreamToLinksIteratee(response, url)
        }.map(_.run).map(_.map { details =>
          sendResults(targets, CrawlResult(url, CrawlHttpStatus(details.response.status), System.nanoTime-start, details.size, details.links))
        }).recover {
          case e : HttpError => sendResults(targets, CrawlResult(url, CrawlHttpStatus(e.status), System.nanoTime-start, 0, Seq.empty))
          case e : UnsupportedContentType => sendResults(targets, CrawlResult(url, SkippedContentType(e.contentType), System.nanoTime() - start, 0, Seq.empty))
          case e : java.util.concurrent.TimeoutException => sendResults(targets, CrawlResult(url, CrawlTimeout(), System.nanoTime() - start, 0, Seq.empty))
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
class SiteActor(val site : String, val concurrency : Int = 2, val ttl : Int) extends Actor {
  val pending = mutable.Set.empty[String]
  var active = Seq.empty[String]
  var visited = Map.empty[String, Long]
  var pathsCrawled = 0
  var stopping = false
  var notified = false
  lazy val robots : RobotsExclusion = fetchRobots()
  val deadLine = System.currentTimeMillis()+ttl*1000;

  def receive = {
    case LinksFound(urls) =>
      urls foreach ( addUrl(_) )
      if(active.isEmpty && !stopping)
        notifyCrawlFinished()

    case CrawlResult(url, status, duration, size, links) =>
      pathsCrawled += 1
      val path = getPath(url)
      active = active.filterNot(_ equals path)
      val time = System.currentTimeMillis

      implicit val ec : ExecutionContext = Akka.system.dispatchers.lookup("play.akka.actor.io-dispatcher")

      Future {
        redis.withClient {
          implicit r =>
            r.zadd("visited:" + site, time, path)
        }
      }
      visited = visited + (getPath(url) -> time)

      val (local, other) = links.partition(site equals LinkUtility.baseUrl(_))

      if(other.nonEmpty) context.parent ! LinksFound(other)

      pending ++= local.map(getPath(_)).filterNot( visited.contains( _ ) )

      if(System.currentTimeMillis < deadLine) {
        var done = false
        while(!done && pending.nonEmpty) {
          val path = pending.head
          pending.remove(path)
          if (shouldCrawl(path)) {
            launchCrawl(path)
            done = true
          }
        }
      }

      if (active.isEmpty) {
        if (stopping) {
          stopActor()
        } else
          notifyCrawlFinished()
      }


    case Stop() =>
      Logger.info("Received Stop signal for "+site)
      stopping = true
      if (active.isEmpty)
        stopActor()

    case msg @ _ => Logger.warn("Unknown message! "+msg)
  }

  override def preStart() {
    redis.withClient { r=>
      r.zrangeWithScore("visited:"+site, 0, -1) map { l =>
        visited = visited ++ l.map { v =>
          val (path, score) = (v._1, v._2)
          (path -> score.asInstanceOf[Long])
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
    pending.clear()
  }

  def notifyCrawlFinished() {
    if (!notified) {
      Logger.info("Notifying manager that site crawl for %s is finished" format (site))
      context.parent ! SiteCrawlFinished(site)
      notified = true
    }
  }

  /**
   * Stop the actor, but first notify about visited sited, and remaining observed sites
   */
  private def stopActor() {
    Logger.info("Stopping site "+site+" context as there is no activity")
    CrawlManager.observed ! ObservedSite(site, pending.toSeq)
    if(pathsCrawled > 0)
      CrawlManager.observed ! VisitedSite(site, visited.keys.toSeq)
    context.stop(self)
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

  def shouldCrawl(path : String) : Boolean = {
    !active.contains(path) && robots.allow(path) && ! visited.contains(path)
  }

  def launchCrawl(path : String) {
    Logger.debug("Site %s launching crawl for %s with %s" format(site, path, self))
    val url = new URL(site+path)
    CrawlManager.crawler ! CrawlRequest(url)
    active = active :+ path
  }

  /**
   * Add url for processing
   * @return Whether url was accepted
   */
  def addUrl(url: URL) {
    val path = getPath(url)
    if (shouldCrawl(path)) {
      if (active.size < concurrency && System.currentTimeMillis < deadLine) {
        launchCrawl(path)
      } else {
        pending += path
      }
    }
  }
}



class CrawlStatisticsActor extends Actor {
  var total = 0
  var success = 0
  var failed = 0
  var ignored = 0
  var timeout = 0
  var redirect = 0
  var totalSites = 0
  var pendingSites = 0
  var duration : Long = 0
  var bytes : Long = 0
  val start = System.nanoTime()
  var listeners = Seq.empty[PushEnumerator[String]]
  var lastStats = CrawlStatistics(0,0,0,0, 0,0, 0, System.nanoTime(),0)

  Akka.system.scheduler.schedule(0 seconds, 1 seconds, self, "tick")

  def receive = {
    case "tick" =>
      CrawlManager.ref ! ManagerStatisticsRequest()

    case CrawlResult(url, status, d, size, links) =>
      total += 1
      bytes += size
      duration += d
      status match {
        case CrawlHttpStatus(200 | 202 | 204) => success += 1
        case CrawlHttpStatus(301 | 302) =>
          Logger.debug("url "+url+" is redirected to "+links)
          redirect += 1
        case CrawlHttpStatus(st) if st >= 400 =>
          failed += 1
          Logger.error("HTTP error "+st+" at "+url)
        case SkippedContentType(contentType) => ignored += 1
        case CrawlTimeout() => timeout += 1
        case _ =>
          failed += 1
          Logger.error("Status needing handling "+status)
      }

    case CrawlStatisticsRequest() =>
      Logger.debug("Received statistics request")
      listeners.foreach(_.push(statsHtml))
      lastStats = CrawlStatistics(total, success, failed, ignored, timeout, redirect, duration, System.nanoTime(), bytes)

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
    val crawlsInPeriod = total-lastStats.total
    val bytesInPeriod = bytes-lastStats.bytes
    val durationInPeriod = duration-lastStats.duration
    "<pre>" +
      ("total sites: active %d, pending %d\ncrawls: total %d, success %d, failure %d, ignored %d, redirect %d, timeout %d, running for %.2fs kB %.2f\n" format (totalSites, pendingSites, total, success, failed, ignored, redirect, timeout, fromStart, bytes/1024.0)) +
      ("from start:  total %.2f 1/s, %.2f kBs, avg response %.2fms, avg page size %.2fkB\n" format (total/fromStart, bytes/fromStart/1024.0, if(total != 0) duration/total/1000000.0 else 0.0, if(total !=0) 1.0*bytes/total else 0.0)) +
      ("last period: total %.2f 1/s, %.2f kBs, avg response %.2fms, avg page size %.2fkB\n" format (crawlsInPeriod/fromLast, bytesInPeriod/fromLast/1024.0, if(crawlsInPeriod!=0) durationInPeriod/crawlsInPeriod/1000000.0 else 0.0, if (crawlsInPeriod!=0) 1.0*bytesInPeriod/crawlsInPeriod else 0.0)) +
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
  implicit val timeout = Timeout(5000)

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
      sender ! Stop()
      active.get(site).filter(_ == sender).foreach { a =>
        Logger.info("Site "+site+" was in list of active crawls")
        active.remove(site) map { actor =>
          Logger.info("Trying to find new site to crawl")
          CrawlManager.observed ! RequestSite()
        }
      }

    case ObservedSite(newSite, paths) =>
      if (active.size<concurrency)
        if (! active.contains(newSite)) {
          launchSiteActor(newSite, paths.flatMap { path =>
            try {
              Some(new URL(newSite+path))
            } catch {
              case _ => None
            }
          }.toSeq)
        }

      if (active.size<concurrency)
        CrawlManager.observed ! RequestSite()

    case ManagerStatisticsRequest() =>
      Logger.info("Responding to ManagerStatisticsRequest")
      sender ! ManagerStatistics(active.size, redisClient.scard("observed_sites").getOrElse(0))

    case msg @ _ => Logger.warn("Unknown message! "+msg)

  }

  def registerLink(urls : Seq[URL]) {
    urls.groupBy {LinkUtility.baseUrl(_)} foreach { a =>
      val (site, siteUrls) = (a._1, a._2)

      active.get(site) match {
        case Some(actor) =>
          actor ! LinksFound(siteUrls)
        case None =>
          if (active.size < concurrency) {
            launchSiteActor(site , siteUrls)
          } else {
            CrawlManager.observed ! ObservedSite(site, siteUrls map {getPath(_)})
          }
      }
    }

  }
  def launchSiteActor(site : String, urls : Seq[URL]) {
    Logger.info("Creating new site actor for "+site)
    val actor = context.actorOf(Props(new SiteActor(site, ttl = Play.configuration.getInt("slurp.site.ttl").getOrElse(300))).withDispatcher("play.akka.actor.site-dispatcher"))
    active.put(site, actor)
    actor ! LinksFound(urls)
  }
  override def preRestart(reason : Throwable , message : Option[Any]) {
    Logger.error("CrawlManager got restarted due %s with message %s" format(reason, message))
    super.preRestart(reason,message)
  }

}



/**
 * Class for managing observed urls
 */
class ObservedManager extends Actor {
  var observed = HashMap.empty[String, HashSet[String]]
  var visited = HashMap.empty[String, HashSet[String]]
  var flusher : Option[Cancellable] = None
  case class FlushData()

  override def preStart() {
    context.system.scheduler.schedule(0 seconds, 5 seconds, self, FlushData())
  }
  override def postStop() {
    flusher.map (_.cancel())
  }

  def receive = {
    case ObservedSite(site, paths) =>
      observed.get(site) match {
        case Some(p) =>
          observed = observed.updated(site, p ++ paths)
        case None =>
          observed = observed.updated(site, HashSet.empty ++ paths)
      }

    case FlushData() =>
      val currentObserved = observed
      val currentVisited = visited
      visited = HashMap.empty
      observed = HashMap.empty

      implicit val ec : ExecutionContext = context.dispatcher

      Future {
        currentObserved.foreach { t =>
          val (site, paths) = (t._1, t._2)
          writeSiteFile(site, paths.toSeq)
          redis.withClient { r =>
            r.sadd("observed_sites", site)
          }
        }

        currentVisited.foreach { t=>
          val (site, paths) = (t._1, t._2)
          val start = System.nanoTime
          val file = siteFile(site)
          val tempFile = siteFile(site+"."+UUID.randomUUID()+".tmp")
          if(file.renameTo(tempFile)) {
            val observed = HashSet.empty ++ Source.fromFile(tempFile).getLines()
            val remaining = observed -- paths
            if (remaining.nonEmpty)
              writeSiteFile(site, remaining.toSeq)
            else
              redis.withClient { r =>
                r.srem("observed_sites", site)
              }
            tempFile.delete()
          }
          Logger.info("Compacting %s took %d ms" format (site, (System.nanoTime-start)/1000/1000))
        }
      }


    case RequestSite() =>
      // First check if we have anything in memory
      if (observed.nonEmpty) {
        val t = observed.head
        val (site, paths) = (t._1, t._2)
        observed = observed - site
        sender ! ObservedSite(site, paths.toSeq)
        site
      } else {
        // If not, then just check from the disk in future
        val origin = sender
        implicit val ec : ExecutionContext = context.dispatcher
        Future {
          redis.withClient { r =>
            var msg : Option[Any] = None
            do {
              msg = r.spop("observed_sites") match {
                case Some(site) =>
                  try {
                    val paths = Source.fromFile(siteFile(site)).getLines().take(500).toList.distinct

                    Some(ObservedSite(site, paths))
                  } catch {
                    case _ => None
                  }
                case None => None
              }
            } while(msg.isEmpty && r.scard("observed_sites").getOrElse(0) > 0)
            msg match {
              case Some(msg) => origin ! msg
              case None => origin ! NoObservedSites()
            }
          }
        }
      }

    case VisitedSite(site, paths) =>
      visited.get(site) match {
        case Some(p) =>
          visited = visited.updated(site, p++paths)
        case None =>
          visited = visited.updated(site, HashSet.empty ++ paths)
      }

      observed.get(site).foreach { p =>
        val remaining = p -- paths
        if(remaining.nonEmpty)
          observed = observed.updated(site, remaining)
        else
          observed = observed - site
      }
  }

  private def writeSiteFile(site : String, paths : Seq[String]) {
    val f = new BufferedWriter(new FileWriter(siteFile(site), true))
    try {
      paths.foreach { path =>
        f.write(path+"\n")
      }
    } finally f.close()

  }
  private def siteFile(site : String) = new File("sites", site.replaceAll("//", ""))

  override def preRestart(reason : Throwable , message : Option[Any]) {
    Logger.error("ObservedManager got restarted due %s with message %s" format(reason, message))
    super.preRestart(reason,message)
  }

}


object CrawlManager {
  lazy val system = Akka.system
  lazy val ref = system.actorOf(Props(new CrawlManager(Play.configuration.getInt("slurp.parallel.sites").getOrElse(100))).withDispatcher("play.akka.actor.manager-dispatcher"), name="manager")
  lazy val statistics = system.actorOf(Props[CrawlStatisticsActor].withDispatcher("play.akka.actor.statistics-dispatcher"), "statistics")
  lazy val crawler = system.actorOf(Props(new CrawlActor(statistics)).withRouter(new SmallestMailboxRouter(2*Runtime.getRuntime.availableProcessors)).withDispatcher("play.akka.actor.crawler-dispatcher"), "crawler")
  lazy val observed = system.actorOf(Props[ObservedManager].withDispatcher("play.akka.actor.io-dispatcher"))

  Akka.system.scheduler.schedule(0 seconds, 5 seconds, statistics, CrawlStatisticsRequest())

  val listener = system.actorOf(Props(new Actor {
    def receive = {
      case d: DeadLetter => Logger.error(d.toString)
    }
  }))
  system.eventStream.subscribe(listener, classOf[DeadLetter])
}
