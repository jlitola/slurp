package controllers

import play.api.{Logger, libs}
import play.api.mvc._
import io.Source
import actors.{Listen, CrawlManager, FeedUrl}
import play.api.libs.iteratee._
import play.api.libs.concurrent._
import libs.Comet
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._


object Application extends Controller {
  
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def uploadUrls() = Action(parse.multipartFormData) { request =>
    request.body.files.map { file =>
      Source.fromFile(file.ref.file).getLines().foreach { line =>
        if(line.startsWith("http://"))
          CrawlManager.ref ! FeedUrl(line)
      }
    }
    Ok("Urls added")
  }

  def statusStream = Action {
    AsyncResult {
      implicit val timeout = Timeout(5.seconds)
      (CrawlManager.statistics ? (Listen())).mapTo[Enumerator[String]].asPromise.map { chunks =>
        Ok.stream(chunks &> Comet(callback = "parent.message"))
      }
    }
  }
}