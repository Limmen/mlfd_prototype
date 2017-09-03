package kth.se.ii2202.mlfd_prototype.actors

import java.text.DecimalFormat
import kth.se.ii2202.mlfd_prototype.actors.DataCollector.NodeDied
import scala.concurrent.duration._

import Superviser._
import Worker._
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}

/*
 * Worker, this actor will respond to hearbeats with simulated geographic delay and simulated network delay and also will simulate probability of crashes
 */
class Worker(id: WorkerId, geoLoc: Integer, randomMillis : Integer, geoFactor : Double, crashProb: Double, collector : ActorRef) extends Actor with ActorLogging with Timers {

  private val random = scala.util.Random
  private val formatter = new DecimalFormat("#.#######################################")

  override def preStart(): Unit = {
    log.debug(s"Worker $id, geo-location: $geoLoc, started")
  }

  /*
   * Receive loop, respond to heartbeats with simulated delays based on
   * randomness and geographic location, also simulate crashes
   */
  def receive = {
    case HeartBeat => {
      log.debug("Worker received heartbeat")
      if(crash()){
        val timeStamp = System.currentTimeMillis().toDouble
        collector ! new NodeDied(List(id.toString(), formatter.format(timeStamp)))
        context.stop(self)
        log.info("worker: " + id + "crashed")
      } else {
        if(!timers.isTimerActive(ReplyDelayTimeoutKey))
          timers.startSingleTimer(ReplyDelayTimeoutKey, ReplyDelayTimeout(sender), getDelay)
      }
    }
    case ReplyDelayTimeout(from) => {
      from ! new HeartBeatReply(id, geoLoc)
    }
  }

  /*
   * Get simulatd delay based on randomness and geo-location
   */
  def getDelay(): FiniteDuration = {
    val geoDelay = (geoLoc * geoFactor)
    val randomDelay = random.nextInt(randomMillis)
    return (geoDelay + randomDelay).millis
  }

  def crash() : Boolean = {
    return random.nextDouble() <= crashProb
  }
}

/*
 * Companion object, i.e static fields
 */
object Worker {
  def props(id: WorkerId, geoLoc: Integer, randomMillis: Integer, geoFactor: Double, crashProb : Double, collector: ActorRef): Props = {
    Props(new Worker(id, geoLoc, randomMillis, geoFactor, crashProb, collector))
  }

  /*
   * HeartBeat reply which includes id and location
   */
  final case class HeartBeatReply(from: WorkerId, loc: GeoLoc)

  type WorkerId = Integer
  type GeoLoc = Integer

  /*
   * Simulate delay with a radom timer
   */
  case class ReplyDelayTimeout(from: ActorRef)
  case object ReplyDelayTimeoutKey

}
