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
class Worker(id: Integer, geoLoc: Double, stdDev : Double, geoFactor : Double,
  crashProb: Double, collector : ActorRef, bandwidth: Double, bandwidthFactor : Double,
  messageLossProb: Double, pattern: Boolean)
    extends Actor with ActorLogging with Timers {

  private val random = new scala.util.Random(id) //Seed with nodeID
  private val formatter = new DecimalFormat("#.#######################################")

  override def preStart(): Unit = {
    log.debug(s"Worker $id, geo-location: $geoLoc, bandwidth: $bandwidth started")
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
      }
      else if(!messageLoss()){
        if(!timers.isTimerActive(ReplyDelayTimeoutKey))
          timers.startSingleTimer(ReplyDelayTimeoutKey, ReplyDelayTimeout(sender), getDelay)
      }
    }
    case ReplyDelayTimeout(from) => {
        from ! new HeartBeatReply(id, geoLoc, bandwidth)
    }
  }

  /*
   * Get simulatd delay based on randomness and geo-location
   */
  def getDelay(): FiniteDuration = {
    var geoDelay : Double = random.nextInt(100) * geoFactor
    var bandwidthDelay : Double = random.nextDouble * bandwidthFactor
    if(pattern){
      geoDelay = (geoLoc * geoFactor).toDouble
      bandwidthDelay = 1 * bandwidthFactor
    }
    if(bandwidth > 0)
      bandwidthDelay = ((1/bandwidth) * bandwidthFactor)
    return ((random.nextGaussian()*stdDev) + geoDelay + bandwidthDelay).millis
  }

  def crash() : Boolean = {
    return random.nextDouble() <= crashProb
  }

  def messageLoss() : Boolean = {
    return random.nextDouble() <= messageLossProb
  }
}

/*
 * Companion object, i.e static fields
 */
object Worker {
  def props(id: Integer, geoLoc: Double, stdDev: Double, geoFactor: Double,
    crashProb : Double, collector: ActorRef, bandwidth: Double, bandwidthFactor: Double,
    messageLossProb: Double, pattern: Boolean): Props = {
    Props(new Worker(id, geoLoc, stdDev, geoFactor, crashProb, collector, bandwidth, bandwidthFactor, messageLossProb, pattern))
  }

  /*
   * HeartBeat reply which includes id and location
   */
  final case class HeartBeatReply(from: Integer, loc: Double, bandwidth: Double)

  /*
   * Simulate delay with a radom timer
   */
  case class ReplyDelayTimeout(from: ActorRef)
  case object ReplyDelayTimeoutKey

}
