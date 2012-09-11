package crawler

import play.api._
import com.redis._

object Global extends GlobalSettings {
  val redis = new RedisClientPool("localhost", 6379)

}
