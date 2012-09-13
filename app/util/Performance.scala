package util

import play.api.Logger


object Performance {
  def time[R](name : String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    Logger.debug("%s took %d µs" format (name, (t1-t0)/1000))
    result
  }


}
